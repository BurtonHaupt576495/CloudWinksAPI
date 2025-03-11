using Microsoft.AspNetCore.Mvc;
using CloudWinksServiceAPI.Data;
using CloudWinksServiceAPI.Models;
using System.Linq;

namespace CloudWinksServiceAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ButtonsController : ControllerBase
    {
        private readonly SpinacheDbContext _context;

        public ButtonsController(SpinacheDbContext context)
        {
            _context = context;
        }

        [HttpGet]
        public IActionResult GetButtons()
        {
            return Ok(_context.CWButtons.ToList());
        }

        [HttpPost]
        public IActionResult CreateButton(CWButton button)
        {
            _context.CWButtons.Add(button);
            _context.SaveChanges();
            return CreatedAtAction(nameof(GetButtons), new { id = button.ButtonId }, button);
        }
    }
}
