using Microsoft.AspNetCore.Mvc;
using CloudWinksServiceAPI.Data;
using CloudWinksServiceAPI.Models;
using System.Linq;
using Microsoft.EntityFrameworkCore;

namespace CloudWinksServiceAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CWAppsController : ControllerBase
    {
        private readonly SpinacheDbContext _context;

        public CWAppsController(SpinacheDbContext context)
        {
            _context = context;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        //[HttpGet]
        //public IActionResult GetCWApps()
        //{
        //    return Ok(_context.CWApps.ToList());
            
        //}
        [HttpGet]
        public async Task<IActionResult> GetCWApps()
        {
            try
            {
                // var apps = new[] { "App1", "App2", "App3" };
                // Fetch _appname from CWApps table asynchronously
                var apps = await _context.CWApps
                    .Select(app => app._appname)
                    .ToListAsync();

                return Ok(apps); // Return the app names as JSON
            }
            catch (Exception ex)
            {
                // Log the error for debugging purposes
                Console.WriteLine($"Error: {ex.Message}");
                return StatusCode(500, "An error occurred while processing your request.");
            }
        }
        [HttpPost]
        public IActionResult CreateCWApp(CWApp app)
        {
            _context.CWApps.Add(app);
            _context.SaveChanges();
            return CreatedAtAction(nameof(GetCWApps), new { id = app._appid }, app);
        }
    }
}
