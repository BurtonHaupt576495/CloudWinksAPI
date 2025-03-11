using Microsoft.AspNetCore.Mvc;
using CloudWinksServiceAPI.Data;
using CloudWinksServiceAPI.Models;
using System.Linq;

namespace CloudWinksServiceAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CWControlsController : ControllerBase
    {
        private readonly SpinacheDbContext _context;

        public CWControlsController(SpinacheDbContext context)
        {
            _context = context;
        }

        [HttpGet]
        public IActionResult GetCWControls()
        {
            return Ok(_context.CWControls.ToList());
        }

        [HttpPost]
        public IActionResult CreateCWControl(CWControl control)
        {
            _context.CWControls.Add(control);
            _context.SaveChanges();
            return CreatedAtAction(nameof(GetCWControls), new { id = control.ControlId }, control);
        }
    }
}
