using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ClassificatorValidator
{
    class Program
    {
        public static void Main()
        {
            RabbitMqService.Configure("localhost", "admin", "admin");

            var factory = new ConnectionFactory() { HostName = "localhost", UserName = "admin", Password = "admin" };

            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: "classificator_files",
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

                    var tempFileMetadata = JsonConvert.DeserializeObject<RabbitMqClassificatorMessage>(message);

                    Console.WriteLine(" [x] Received {0}", tempFileMetadata.Guid);

                    CheckIfNewClassificatorNeeded(tempFileMetadata);

                    channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);

                    await RabbitMqService.EnqueueObject(tempFileMetadata, "ready_files");

                    Console.WriteLine(" [x] Sent to Spark {0}", tempFileMetadata.Guid);
                };

                while (true)
                {
                    try
                    {
                        channel.BasicConsume(queue: "classificator_files",
                         autoAck: false,
                         consumer: consumer);

                        Thread.Sleep(200);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Error on something...\n{0}", e.ToString());
                    }

                }

            }
        }

        private static void CheckIfNewClassificatorNeeded(RabbitMqClassificatorMessage tempFileMetadata)
        {
            string sparkAddress = @"spark://10.0.75.1:7077";

            Process proc = new Process
            {

                StartInfo = new ProcessStartInfo
                {
                    FileName = @"C:\spark\bin\spark-submit.cmd",
                    Arguments = $@"--master {sparkAddress} C:\recognition\sparkscript.py -i {tempFileMetadata.Guid} -p C:\recognition\model.txt -m C:\recognition\model.caffemodel",
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                    CreateNoWindow = true
                }
            };

            proc.Start();

            while (!proc.StandardOutput.EndOfStream)
            {
                string line = proc.StandardOutput.ReadLine();
                Console.WriteLine(line);
            }
        }



    }

    public class RabbitMqFileMessage
    {
        public string FilePath { get; set; }
        public string Extension { get; set; }
        public string FileName { get; set; }
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
