using Microsoft.AspNetCore.Mvc;
using CloudWinksServiceAPI.Data;
using CloudWinksServiceAPI.Models;
using System.Linq;

namespace CloudWinksServiceAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CWActionsController : ControllerBase
    {
        private readonly SpinacheDbContext _context;

        public CWActionsController(SpinacheDbContext context)
        {
            _context = context;
        }

        [HttpGet]
        public IActionResult GetCWActions()
        {
            return Ok(_context.CWActions.ToList());
        }

        [HttpPost]
        public IActionResult CreateCWAction(CWAction action)
        {
            _context.CWActions.Add(action);
            _context.SaveChanges();
            return CreatedAtAction(nameof(GetCWActions), new { id = action.ActionId }, action);
        }
    }
}
