using System;
using System.Collections.Generic;
using System.Text;
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

        public async static Task EnqueueFile(string path, string extension, string fileName, string queueName, RabbitMqConnectionSettings settings = null)
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

                    var message = JsonConvert.SerializeObject(model);
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
