using Mgr.Common.Models;
using Mgr.Common.Services;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;
using System.Threading;

namespace VideoNormalizer
{
    class Program
    {
        public static void Main()
        {
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

                    var newMetadata = NormalizeVideoFile(tempFileMetadata);
                    var splitFileMetadata = SplitIntoFramesAndSaveOnServer(newMetadata);

                    channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);

                    await RabbitMqService.EnqueueFile(splitFileMetadata.FilePath, splitFileMetadata.Extension, splitFileMetadata.FileName, "classificator_files");

                    Console.WriteLine(" [x] Sent to classificator judgement {0}", splitFileMetadata.FileName);
                };

                while(true)
                {
                    try
                    {
                        channel.BasicConsume(queue: "task_queue",
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

        private static RabbitMqFileMessage NormalizeVideoFile(RabbitMqFileMessage tempFileMetadata)
        {
            var newMetadata = new RabbitMqFileMessage();



            return newMetadata;
        }
    }
}
