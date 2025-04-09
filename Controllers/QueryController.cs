using Microsoft.AspNetCore.Mvc;
using Npgsql;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using System.Collections.Generic;
using System.Data;

namespace CloudWinksServiceAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class QueryController : ControllerBase
    {
        private readonly IConfiguration _configuration;
        private readonly DatabaseConnectionManager _connectionManager;

        public QueryController(IConfiguration configuration, DatabaseConnectionManager connectionManager)
        {
            _configuration = configuration;
            _connectionManager = connectionManager;
        }

        [HttpPost("GenericExecute")]
        public async Task<IActionResult> GenericExecute([FromBody] ExecuteRequest request)
        {
            if (request == null || request.AppId <= 0 || string.IsNullOrWhiteSpace(request.Name))
            {
                return BadRequest("Invalid request data.");
            }

            Console.WriteLine("✅ Received GenericExecute request:");
            Console.WriteLine($"AppId: {request.AppId}");
            Console.WriteLine($"Name: {request.Name}");
            Console.WriteLine($"Parameters: {JsonSerializer.Serialize(request.Parameters)}");

            try
            {
                string connectionString = _connectionManager.GetOrAddConnectionString(request.AppId);
                Console.WriteLine($"Using Connection String: {connectionString}");

                using (var connection = new NpgsqlConnection(connectionString))
                {
                    await connection.OpenAsync();
                    Console.WriteLine("Connection opened successfully.");

                    var isStoredProcedure = IsStoredProcedure(request.Name, connection);

                    using (var command = connection.CreateCommand())
                    {
                        if (isStoredProcedure)
                        {
                            if (request.Parameters != null && request.Parameters.Any())
                            {
                                // Check if any parameter value is a dictionary with "type" and "value" (PostgreSQL case)
                                bool isPostgresFormat = request.Parameters.Any(p =>
                                    p.Value is Dictionary<string, object> dict &&
                                    dict.ContainsKey("type") && dict.ContainsKey("value"));

                                if (isPostgresFormat)
                                {
                                    // PostgreSQL case (valuePairs = false)
                                    var paramList = new List<string>();
                                    int paramIndex = 1;
                                    foreach (var param in request.Parameters)
                                    {
                                        var paramData = param.Value as Dictionary<string, object>;
                                        if (paramData != null && paramData.ContainsKey("type") && paramData.ContainsKey("value"))
                                        {
                                            string paramType = paramData["type"]?.ToString() ?? "text";
                                            var paramValue = ConvertJsonElement(paramData["value"]);
                                            paramList.Add($"${paramIndex}::{paramType}");
                                            Console.WriteLine($"Parameter {param.Key}: Type={paramType}, Value={paramValue}");
                                            command.Parameters.AddWithValue("", paramValue ?? DBNull.Value);
                                            paramIndex++;
                                        }
                                    }
                                    command.CommandText = $"SELECT dbo.\"{request.Name}\"({string.Join(", ", paramList)})";
                                    command.CommandType = CommandType.Text;
                                }
                                else
                                {
                                    // MSSQL case (valuePairs = true)
                                    var paramString = string.Join(", ", request.Parameters.Select(p => $"@{p.Key}"));
                                    command.CommandText = $"SELECT dbo.\"{request.Name}\"({paramString})";
                                    command.CommandType = CommandType.Text;

                                    foreach (var param in request.Parameters)
                                    {
                                        var value = ConvertJsonElement(param.Value);
                                        Console.WriteLine($"Parameter {param.Key}: {value}");
                                        command.Parameters.AddWithValue(param.Key, value ?? DBNull.Value);
                                    }
                                }
                            }
                            else
                            {
                                command.CommandText = $"SELECT dbo.\"{request.Name}\"()";
                                command.CommandType = CommandType.Text;
                            }
                        }
                        else
                        {
                            // Handle table/view query (unchanged)
                            command.CommandText = $"SELECT * FROM {request.Name}";
                            command.CommandType = CommandType.Text;
                        }

                        var jsonResult = await command.ExecuteScalarAsync();
                        Console.WriteLine($"Raw JSON result: {jsonResult}");

                        if (jsonResult is string jsonString)
                        {
                            return Ok(JsonSerializer.Deserialize<object>(jsonString));
                        }
                        return Ok(new object[0]);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
                Console.WriteLine($"Stack Trace: {ex.StackTrace}");
                return StatusCode(500, ex.Message);
            }
        }

        private bool IsStoredProcedure(string name, NpgsqlConnection connection)
        {
            var query = "SELECT COUNT(*) FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid WHERE n.nspname = 'dbo' AND p.proname = LOWER(@name)";
            using (var command = new NpgsqlCommand(query, connection))
            {
                command.Parameters.AddWithValue("@name", name.ToLower());
                var result = command.ExecuteScalar();
                return result != null && (long)result > 0;
            }
        }

        private object? ConvertJsonElement(object? value)
        {
            if (value is JsonElement jsonElement)
            {
                switch (jsonElement.ValueKind)
                {
                    case JsonValueKind.String: return jsonElement.GetString() ?? string.Empty;
                    case JsonValueKind.Number:
                        if (jsonElement.TryGetInt32(out int intValue)) return intValue;
                        if (jsonElement.TryGetDouble(out double doubleValue)) return doubleValue;
                        return jsonElement.GetDecimal();
                    case JsonValueKind.True: return true;
                    case JsonValueKind.False: return false;
                    case JsonValueKind.Null: return null;
                    default: throw new InvalidOperationException($"Unsupported JsonElement type: {jsonElement.ValueKind}");
                }
            }
            return value;
        }

        public class ExecuteRequest
        {
            public int AppId { get; set; }
            public string Name { get; set; } = string.Empty;
            public Dictionary<string, object> Parameters { get; set; } = [];
        }
    }
}