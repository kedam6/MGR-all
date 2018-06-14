using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Mgr.Common.Models;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Mgr.Common.Services
{
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

        public async static Task EnqueueFile(string path, string extension, string fileName, string guid, string queueName, RabbitMqConnectionSettings settings = null)
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

                    var model = new RabbitMqFileMessage();
                    model.FilePath = path;
                    model.FileName = fileName;
                    model.Extension = extension;
                    model.Guid = guid;

                    var message = JsonConvert.SerializeObject(model);
                    var body = Encoding.UTF8.GetBytes(message);

                    channel.BasicPublish(exchange: "",
                                         routingKey: queueName,
                                         basicProperties: null,
                                         body: body);
                }
            }


        }

        public async static Task<RetSet> GetResultFromSpark(string guid, RabbitMqConnectionSettings settings = null)
        {
            var connFactory = new ConnectionFactory();
            RetSet ret = null;

            if (settings == null)
            {
                settings = defaultSettings;
            }

            connFactory.HostName = settings.Host;
            connFactory.UserName = settings.Username;
            connFactory.Password = settings.Password;


            using (var connection = connFactory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: "done_items",
                                     durable: true,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);

                Console.WriteLine(" [*] Waiting for response...");

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += async (model, ea) =>
                {
                    var body = ea.Body;
                    var message = Encoding.UTF8.GetString(body);

                    var data = JsonConvert.DeserializeObject<ResultSet>(message);

                    if (data.Guid != guid)
                        channel.BasicReject(0, true);
                    else
                    {
                        ret = new RetSet
                        {
                            Occurences = data.Occurences.ToDictionary(x => x.Key, x => x.Value)
                        };
                        channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                    }


                };

                while (true)
                {
                    try
                    {
                        channel.BasicConsume(queue: "done_items",
                         autoAck: false,
                         consumer: consumer);

                        if (ret != null)
                            return ret;
                        Thread.Sleep(200);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Error on something...\n{0}", e.ToString());
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

    public class ResultSet
    {
        public string Guid { get; set; }
        public Dictionary<string, int> Occurences { get; set; }
    }

    public class RetSet
    {
        public Dictionary<string, int> Occurences { get; set; }
    }
}

    