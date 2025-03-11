using Microsoft.AspNetCore.Mvc;
using CloudWinksServiceAPI.Models;
using System.Threading.Tasks;
using CloudWinksServiceAPI.Interfaces;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using System.Data;
using System.Text.Json;
using System.Collections.Generic;

namespace CloudWinksServiceAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class QueryController : ControllerBase
    {
        private readonly IDynamicQueryService _dynamicQueryService;
        private readonly IConfiguration _configuration;
        private readonly DatabaseConnectionManager _connectionManager;

        public QueryController(IDynamicQueryService dynamicQueryService, IConfiguration configuration, DatabaseConnectionManager connectionManager)
        {
            _dynamicQueryService = dynamicQueryService;
            _configuration = configuration;
            _connectionManager = connectionManager;
        }

      

        [HttpPost("GenericExecute")]
        public async Task<IActionResult> GenericExecute([FromBody] ExecuteRequest request)
        {
            if (request == null || request.AppId <= 0 || string.IsNullOrWhiteSpace(request.Name))
            {
                return BadRequest(new { status = "error", message = "Invalid request data." });
            }

            try
            {
                // Log appId and name
                Console.WriteLine($"AppId: {request.AppId}, Name: {request.Name}");

                // Get connection string
                string connectionString = _connectionManager.GetOrAddConnectionString(request.AppId);
                Console.WriteLine($"Using Connection String: {connectionString}");

                using (var connection = new SqlConnection(connectionString))
                {
                    await connection.OpenAsync();
                    Console.WriteLine("Connection opened successfully.");

                    var isStoredProcedure = IsStoredProcedure(request.Name, connection);

                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = isStoredProcedure
                            ? request.Name
                            : $"SELECT * FROM [{request.Name}] FOR JSON PATH";
                        command.CommandType = isStoredProcedure ? CommandType.StoredProcedure : CommandType.Text;

                        if (request.Parameters != null && isStoredProcedure)
                        {
                            foreach (var param in request.Parameters)
                            {
                                var value = ConvertJsonElement(param.Value);
                                command.Parameters.AddWithValue(param.Key, value ?? DBNull.Value);
                            }
                        }

                        using (var reader = await command.ExecuteReaderAsync())
                        {
                            var results = new List<object>();
                            while (await reader.ReadAsync())
                            {
                                var row = new Dictionary<string, object>();
                                for (int i = 0; i < reader.FieldCount; i++)
                                {
                                    row[reader.GetName(i)] = reader.GetValue(i);
                                }
                                results.Add(row);
                            }

                            Console.WriteLine("Data retrieved successfully.");
                            return Ok(new { status = "success", data = results });
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
                return StatusCode(500, new { status = "error", message = ex.Message });
            }
        }


        [HttpPost("query")]
        public async Task<IActionResult> ExecuteQuery([FromBody] QueryRequest request)
        {
            if (request.Parameters == null)
            {
                request.Parameters = new dbFields();
            }
            var result = await _dynamicQueryService.ExecuteQueryWithParametersAsync(request.TableName, request.Parameters);
            return Ok(new ApiResponse<dbQueryResult>
            {
                Status = "Success",
                Data = result
            });
        }

        private bool IsStoredProcedure(string name, SqlConnection connection)
        {
            var query = "SELECT COUNT(*) FROM sys.procedures WHERE name = @name";
            using (var command = new SqlCommand(query, connection))
            {
                command.Parameters.AddWithValue("@name", name);
                return (int)command.ExecuteScalar() > 0;
            }
        }

        private object ConvertJsonElement(object value)
        {
            if (value is JsonElement jsonElement)
            {
                switch (jsonElement.ValueKind)
                {
                    case JsonValueKind.String:
                        return jsonElement.GetString();
                    case JsonValueKind.Number:
                        if (jsonElement.TryGetInt32(out int intValue)) return intValue;
                        if (jsonElement.TryGetDouble(out double doubleValue)) return doubleValue;
                        return jsonElement.GetDecimal();
                    case JsonValueKind.True:
                    case JsonValueKind.False:
                        return jsonElement.GetBoolean();
                    case JsonValueKind.Null:
                        return null;
                    default:
                        throw new InvalidOperationException($"Unsupported JsonElement type: {jsonElement.ValueKind}");
                }
            }
            return value;
        }
    }

    // Define the ExecuteRequest class
    public class ExecuteRequest
    {
        public int AppId { get; set; } // Application ID for the connection string
        public string Name { get; set; } // Stored procedure or table name
        public Dictionary<string, object> Parameters { get; set; } // Parameters for execution
    }

    // Define the QueryRequest class
    public class QueryRequest
    {
        public string TableName { get; set; } // Table name
        public dbFields Parameters { get; set; } // Query parameters
    }

    // Add additional class definitions, if necessary
}
