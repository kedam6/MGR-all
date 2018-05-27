using Microsoft.AspNetCore.Http;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace Mgr.Common.Helpers
{
    public class TempFileHelper
    {
        public async static Task<string> SaveToTempFolder(IFormFile file)
        {
            var tempPath = Path.GetTempPath();
            var filePath = Path.Combine(tempPath, file.FileName);


            if (file.Length > 0)
            {
                using (var stream = new FileStream(filePath, FileMode.Create))
                //using (var stream = new MemoryStream())
                {
                    await file.CopyToAsync(stream);
                }
            }

            return filePath;
        }
    }
}
