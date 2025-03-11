using Microsoft.AspNetCore.Mvc;
using CloudWinksServiceAPI.Data;
using CloudWinksServiceAPI.Models;
using System.Linq;

namespace CloudWinksServiceAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ConstantsController : ControllerBase
    {
        private readonly SpinacheDbContext _context;

        public ConstantsController(SpinacheDbContext context)
        {
            _context = context;
        }

        [HttpGet]
        public IActionResult GetConstants()
        {
            return Ok(_context.CWConstants.ToList());
        }

        [HttpPost]
        public IActionResult CreateConstant(CWConstant constant)
        {
            _context.CWConstants.Add(constant);
            _context.SaveChanges();
            return CreatedAtAction(nameof(GetConstants), new { name = constant.ConstantName }, constant);
        }
    }
}
