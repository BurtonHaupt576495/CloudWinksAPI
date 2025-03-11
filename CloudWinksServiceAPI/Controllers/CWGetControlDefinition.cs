using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using System.Data;
using System.Text.Json;

[ApiController]
[Route("api/[controller]")]
public class QueryController : ControllerBase
{
    private readonly string _connectionString = "Server=localhost;Database=Spinache;User=developer;Password=TrumpFor2024;MultipleActiveResultSets=true;TrustServerCertificate=True";

    [HttpPost]
    [Route("CWGetControlDefinition")]
    public IActionResult GetControlDefinition([FromBody] ControlRequest request)
    {
        try
        {
            // Create the SQL connection
            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();

                // Set up the SQL command
                using (var command = new SqlCommand("CWGetControlDefinition", connection))
                {
                    command.CommandType = CommandType.StoredProcedure;

                    // Add the parameter for the stored procedure
                    command.Parameters.Add(new SqlParameter("@ControlID", request.ControlID));

                    // Execute the command
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            reader.Read();
                            // Get the JSON result from the stored procedure
                            var resultJson = reader.GetString(0);
                            var resultObject = JsonSerializer.Deserialize<object>(resultJson);
                            return Ok(resultObject); // Return as a JSON object
                        }
                        else
                        {
                            return NotFound(new { message = "Control definition not found." });
                        }
                    }
                }
            }
        }
        catch (Exception ex)
        {
            // Handle any errors
            return StatusCode(500, new { message = "An error occurred.", error = ex.Message });
        }
    }
}

// DTO for the request
public class ControlRequest
{
    public int ControlID { get; set; }
}
