using Microsoft.AspNetCore.Mvc;
using CloudWinksServiceAPI.Data;
using CloudWinksServiceAPI.Models;
using System.Linq;

namespace CloudWinksServiceAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CWFieldsController : ControllerBase
    {
        private readonly SpinacheDbContext _context;

        public CWFieldsController(SpinacheDbContext context)
        {
            _context = context;
        }

        [HttpGet]
        public IActionResult GetCWFields()
        {
            return Ok(_context.CWFields.ToList());
        }

        [HttpPost]
        public IActionResult CreateCWField(CWField field)
        {
            _context.CWFields.Add(field);
            _context.SaveChanges();
            return CreatedAtAction(nameof(GetCWFields), new { id = field.FieldId }, field);
        }
    }
}
