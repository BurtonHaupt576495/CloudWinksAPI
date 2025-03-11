using Microsoft.AspNetCore.Mvc;
using CloudWinksServiceAPI.Data;
using CloudWinksServiceAPI.Models;
using System.Linq;

namespace CloudWinksServiceAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CWParamsController : ControllerBase
    {
        private readonly SpinacheDbContext _context;

        public CWParamsController(SpinacheDbContext context)
        {
            _context = context;
        }

        [HttpGet]
        public IActionResult GetCWParams()
        {
            return Ok(_context.CWParams.ToList());
        }

        [HttpPost]
        public IActionResult CreateCWParam(CWParam param)
        {
            _context.CWParams.Add(param);
            _context.SaveChanges();
            return CreatedAtAction(nameof(GetCWParams), new { id = param.ParamId }, param);
        }
    }
}
