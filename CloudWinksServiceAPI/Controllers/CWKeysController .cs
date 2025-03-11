using Microsoft.AspNetCore.Mvc;
using CloudWinksServiceAPI.Data;
using CloudWinksServiceAPI.Models;
using System.Linq;

namespace CloudWinksServiceAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CWKeysController : ControllerBase
    {
        private readonly SpinacheDbContext _context;

        public CWKeysController(SpinacheDbContext context)
        {
            _context = context;
        }

        [HttpGet]
        public IActionResult GetCWKeys()
        {
            return Ok(_context.CWKeys.ToList());
        }

        [HttpPost]
        public IActionResult CreateCWKey(CWKey key)
        {
            _context.CWKeys.Add(key);
            _context.SaveChanges();
            return CreatedAtAction(nameof(GetCWKeys), new { id = key.KeyId }, key);
        }
    }
}
