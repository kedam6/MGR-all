using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Mgr.Client.Attributes;
using Mgr.Client.Helpers;
using Mgr.Client.Mapping;
using Mgr.Client.ViewModels;
using Mgr.Common.Helpers;
using Mgr.Common.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.ModelBinding;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Extensions.Logging;
using Microsoft.Net.Http.Headers;

namespace Mgr.Client.Controllers
{
    [Route("/Upload")]
    public class UploadController : Controller
    {
        private readonly ILogger<UploadController> _logger;

        // Get the default form options so that we can use them to set the default limits for
        // request body data
        private static readonly FormOptions _defaultFormOptions = new FormOptions();

        public UploadController(ILogger<UploadController> logger)
        {
            _logger = logger;
        }


        public async Task<IActionResult> Post(IFormFile file)
        {

            if(file != null)
            {
                var extension = Path.GetExtension(file.FileName);

                if(AllowedExtensions.IsAllowedExtension(extension))
                {
                    var path = await TempFileHelper.SaveToTempFolder(file);
                    var guidOfAction = Guid.NewGuid();

                    await RabbitMqService.EnqueueFile(path, extension, file.FileName, guidOfAction.ToString(), "temp_files");

                    var returned = await RabbitMqService.GetResultFromSpark(guidOfAction.ToString());

                    returned.Occurences = returned.Occurences.Where(x => x.Value != 0).ToDictionary(x => x.Key, x => x.Value);

                    ViewBag.Ret = returned;
                    return View(returned);
                    //return Ok(returned);
                }




            }

            return BadRequest("Bad file extension");
        }
    }
}