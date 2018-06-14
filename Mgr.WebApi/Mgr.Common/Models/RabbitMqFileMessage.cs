using System;
using System.Collections.Generic;
using System.Text;

namespace Mgr.Common.Models
{
    public class RabbitMqFileMessage
    {
        public string FilePath { get; set; }
        public string Extension { get; set; }
        public string FileName { get; set; }
        public string Guid { get; set; }
    }
}
