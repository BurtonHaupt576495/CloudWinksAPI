using Microsoft.AspNetCore.Mvc;
using CloudWinksServiceAPI.Data;
using CloudWinksServiceAPI.Models;
using System.Linq;
using Microsoft.EntityFrameworkCore;

namespace CloudWinksServiceAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class TestController : ControllerBase
    {
        private readonly SpinacheDbContext _context;

        public TestController(SpinacheDbContext context)
        {
            _context = context;
        }

        [HttpGet]
        public async Task<IActionResult> TestApps()
        {
            try
            {
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
    }
}