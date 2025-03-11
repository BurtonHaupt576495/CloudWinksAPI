using Microsoft.AspNetCore.Mvc;
using CloudWinksServiceAPI.Data;
using CloudWinksServiceAPI.Models;
using System.Linq;

namespace CloudWinksServiceAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CWMenuTabOptionsController : ControllerBase
    {
        private readonly SpinacheDbContext _context;

        public CWMenuTabOptionsController(SpinacheDbContext context)
        {
            _context = context;
        }

        [HttpGet]
        public IActionResult GetCWMenuTabOptions()
        {
            return Ok(_context.CWMenuTabOptions.ToList());
        }

        [HttpPost]
        public IActionResult CreateCWMenuTabOption(CWMenuTabOption menuTabOption)
        {
            _context.CWMenuTabOptions.Add(menuTabOption);
            _context.SaveChanges();
            return CreatedAtAction(nameof(GetCWMenuTabOptions), new { id = menuTabOption.OptionId }, menuTabOption);
        }
    }
}