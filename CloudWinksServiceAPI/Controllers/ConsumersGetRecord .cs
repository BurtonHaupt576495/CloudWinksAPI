using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using System.Data;
using System.Threading.Tasks;

[Route("api/[controller]")]
[ApiController]
public class ConsumersController : ControllerBase
{
    private readonly string _connectionString;

    public ConsumersController(IConfiguration configuration)
    {
        _connectionString = configuration.GetConnectionString("ZibitDatabase");
    }

    [HttpPost("GetConsumer")]
    public async Task<IActionResult> GetConsumer([FromBody] ConsumerRequest request)
    {
        if (string.IsNullOrEmpty(request.Mobile))
        {
            return BadRequest("Mobile number is required.");
        }

        try
        {
            using (SqlConnection conn = new SqlConnection(_connectionString))
            {
                await conn.OpenAsync();

                using (SqlCommand cmd = new SqlCommand("dbo.ConsumersGetRecord", conn))
                {
                    cmd.CommandType = CommandType.StoredProcedure;

                    // Add parameters to the stored procedure
                   // cmd.Parameters.AddWithValue("@_userid", request.UserId ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@_mobile", request.Mobile);

                    // Execute the stored procedure and fetch results
                    using (SqlDataReader reader = await cmd.ExecuteReaderAsync())
                    {
                        if (await reader.ReadAsync())
                        {
                            var consumerData = new
                            {
                                ConsumerId = reader.GetInt32(0),
                                DisplayName = reader.GetString(1),
                                SweetMoney = reader.GetDecimal(2),
                                MobileMoney = reader.GetDecimal(3),
                                GiftMoney = reader.GetDecimal(4),
                                CrisisMoney = reader.GetDecimal(5),
                                Manager = reader.IsDBNull(6) ? null : reader.GetSqlBinary(6).Value,
                                Rep = reader.IsDBNull(7) ? null : reader.GetSqlBinary(7).Value,
                                Vendor = reader.IsDBNull(8) ? null : reader.GetSqlBinary(8).Value,
                                Consumer = reader.GetInt32(9),
                                FirstLoginDate = reader.GetDateTime(10),
                                QtyManager = reader.GetInt32(11),
                                QtyRep = reader.GetInt32(12),
                                QtyVendor = reader.GetInt32(13),
                                LoginCount = reader.GetInt32(14)
                            };

                            return Ok(consumerData);
                        }
                        else
                        {
                            return NotFound("Consumer not found.");
                        }
                    }
                }
            }
        }
        catch (Exception ex)
        {
            return StatusCode(500, $"Internal server error: {ex.Message}");
        }
    }
}

public class ConsumerRequest
{
   // public int? UserId { get; set; } // UserId can be optional
    public required string Mobile { get; set; }
}
