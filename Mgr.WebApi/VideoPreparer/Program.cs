using Accord.Video.FFMPEG;
using Microsoft.Hadoop.WebHDFS;
using Newtonsoft.Json;
using NReco.VideoConverter;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Drawing;
using System.Drawing.Imaging;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace VideoPreparer
{
    class Program
    {
        private static FFMpegConverter converter = new FFMpegConverter();


        public static void Main()
        {
            RabbitMqService.Configure("localhost", "admin", "admin");

            var factory = new ConnectionFactory() { HostName = "localhost", UserName = "admin", Password = "admin" };

            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: "temp_files",
                                     durable: true,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);

                Console.WriteLine(" [*] Waiting for messages.");

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += async (model, ea) =>
                {
                    var body = ea.Body;
                    var message = Encoding.UTF8.GetString(body);

                    var tempFileMetadata = JsonConvert.DeserializeObject<RabbitMqFileMessage>(message);

                    Console.WriteLine(" [x] Received {0}", tempFileMetadata.FileName);
                    
                    if(!File.Exists(tempFileMetadata.FilePath))
                    {
                        channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                        return;
                    }


                    var newMetadata = NormalizeVideoFile(tempFileMetadata);
                    var splitFileMetadata = SplitIntoFramesAndSaveOnServer(newMetadata);

                    channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);

                    await RabbitMqService.EnqueueObject(splitFileMetadata, "classificator_files");

                    Console.WriteLine(" [x] Sent to classificator judgement {0}", splitFileMetadata.Guid);
                };

                while (true)
                {
                    try
                    {
                        channel.BasicConsume(queue: "temp_files",
                         autoAck: false,
                         consumer: consumer);

                        Thread.Sleep(200);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Error on something...\n{0}", e.ToString());
                        Thread.Sleep(2000);
                    }

                }

            }
        }

        private static RabbitMqClassificatorMessage SplitIntoFramesAndSaveOnServer(RabbitMqFileMessage metadata)
        {
            try
            {
                var newMetadata = new RabbitMqClassificatorMessage();
                Uri myUri = new Uri("http://localhost:50070");
                string userName = "Quinn";
                WebHDFSClient myClient = new WebHDFSClient(myUri, userName);


                var frames = GetFrames(metadata.FilePath).ToArray();

                var newGuid = metadata.Guid;
                var newPath = $"C:\\Vids\\{newGuid.ToString()}\\";

                if (!Directory.Exists(newPath))
                    Directory.CreateDirectory(newPath);

                myClient.CreateDirectory("/" + newGuid.ToString()).Wait();


                for (int i = 0; i < frames.Length; i++)
                {
                    string srcPath = Path.Combine(newPath, $"{i}.jpg");
                    frames[i].Save(Path.Combine(newPath, $"{i}.jpg"), ImageFormat.Jpeg);
                    myClient.CreateFile(srcPath, "/" + newGuid.ToString() + "/" + $"{i}.jpg").Wait();
                }

                File.WriteAllLines(Path.Combine(newPath, "info.ini"), new string[] { frames.Length.ToString() });
                myClient.CreateFile(Path.Combine(newPath, "info.ini"), "/" + newGuid.ToString() + "/" + "info.ini").Wait();

                File.Delete(metadata.FilePath);

                //list file contents of destination directory
                Console.WriteLine();
                Console.WriteLine("Contents of " + newGuid.ToString());

                myClient.GetDirectoryStatus("/" + newGuid.ToString()).ContinueWith(
                     ds => ds.Result.Files.ToList().ForEach(
                     f => Console.Write(" " + f.PathSuffix)
                     )).ContinueWith(x => Console.WriteLine());

                newMetadata.Guid = newGuid.ToString();
                newMetadata.ClassificatorsUsed = string.Empty;//metadata.Classificators;


                return newMetadata;
            }
            catch (Exception e)
            {

                throw e;
            }

        }

        private static RabbitMqFileMessage NormalizeVideoFile(RabbitMqFileMessage tempFileMetadata)
        {
            var newMetadata = new RabbitMqFileMessage();

            var newFilePath = ConvertVideoOrAnimationToMp4(tempFileMetadata.FilePath);

            newMetadata.FilePath = newFilePath;
            newMetadata.Extension = Path.GetExtension(newFilePath);
            newMetadata.FileName = Path.GetFileName(newFilePath);
            newMetadata.Guid = tempFileMetadata.Guid;


            return newMetadata;
        }





        static Program()
        {
            converter.ConvertProgress += Converter_ConvertProgress;
        }

        private static void Converter_ConvertProgress(object sender, ConvertProgressEventArgs e)
        {
            Console.WriteLine("{0}, {1}", e.Processed, e.TotalDuration);
        }

        public static string ConvertVideoOrAnimationToMp4(string filePath)
        {
            var newPath = Path.ChangeExtension(filePath, ".mp4");

            converter.ConvertMedia(filePath, newPath, "mp4");
            File.Delete(filePath);


            return newPath;
        }

        public static IEnumerable<Bitmap> GetFrames(string filePath)
        {
            using (var vFReader = new VideoFileReader())
            {
                vFReader.Open(filePath);
                for (int i = 0; i < vFReader.FrameCount; i++)
                {
                    Bitmap frame = vFReader.ReadVideoFrame();
                    yield return frame;
                }
                vFReader.Close();
            }
        }
    }

    public class RabbitMqFileMessage
    {
        public string FilePath { get; set; }
        public string Extension { get; set; }
        public string FileName { get; set; }
        public string Guid { get; set; }
    }

    public class RabbitMqClassificatorMessage
    {
        public string Guid { get; set; }
        public string ClassificatorsUsed { get; set; }
    }

    public class RabbitMqService
    {
        private static RabbitMqConnectionSettings defaultSettings;

        public static void Configure(string host, string username, string pass)
        {
            defaultSettings = new RabbitMqConnectionSettings
            {
                Host = host,
                Password = pass,
                Username = username
            };


        }

        public async static Task EnqueueObject(object item, string queueName, RabbitMqConnectionSettings settings = null)
        {
            var connFactory = new ConnectionFactory();

            if (settings == null)
            {
                settings = defaultSettings;
            }

            connFactory.HostName = settings.Host;
            connFactory.UserName = settings.Username;
            connFactory.Password = settings.Password;


            using (var connection = connFactory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    channel.QueueDeclare(queue: queueName,
                                         durable: true,
                                         exclusive: false,
                                         autoDelete: false,
                                         arguments: null);


                    var message = JsonConvert.SerializeObject(item);
                    var body = Encoding.UTF8.GetBytes(message);

                    channel.BasicPublish(exchange: "",
                                         routingKey: queueName,
                                         basicProperties: null,
                                         body: body);
                }
            }


        }
    }

    public class RabbitMqConnectionSettings
    {
        public string Host { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }
    }
}
