using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Mgr.Client.Mapping
{
    public static class AllowedExtensions
    {
        private static List<string> allowedExtensions = new List<string>
        {
            ".gif",
            ".mp4",
            ".avi",
            ".mkv",
            ".gfy"
        };

        public static bool IsAllowedExtension(string type)
        {
            return allowedExtensions.Any(x => x == type);
        }
    }
}
