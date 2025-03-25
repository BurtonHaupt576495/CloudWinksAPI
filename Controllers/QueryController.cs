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
                return BadRequest(new { status = "error", message = "Invalid request data." });
            }

            try
            {
                Console.WriteLine($"AppId: {request.AppId}, Name: {request.Name}");
                string connectionString = _connectionManager.GetOrAddConnectionString(request.AppId);
                Console.WriteLine($"Using Connection String: {connectionString}");

                using (var connection = new NpgsqlConnection(connectionString))
                {
                    await connection.OpenAsync();
                    Console.WriteLine("Connection opened successfully.");

                    var isStoredProcedure = IsStoredProcedure(request.Name, connection);

                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = isStoredProcedure
                            ? $"SELECT dbo.\"{request.Name}\"()"
                            : $"SELECT * FROM {request.Name}";
                        command.CommandType = CommandType.Text;

                        if (request.Parameters != null && request.Parameters.Any() && isStoredProcedure)
                        {
                            var paramString = string.Join(", ", request.Parameters.Select(p => $"@{p.Key}"));
                            command.CommandText = $"SELECT dbo.\"{request.Name}\"({paramString})";
                            foreach (var param in request.Parameters)
                            {
                                var value = ConvertJsonElement(param.Value);
                                Console.WriteLine($"Parameter {param.Key}: {value}");
                                command.Parameters.AddWithValue(param.Key, value ?? DBNull.Value);
                            }
                        }

                        var jsonResult = await command.ExecuteScalarAsync();
                        Console.WriteLine($"Raw JSON result: {jsonResult}");

                        if (jsonResult is string jsonString)
                        {
                            var deserializedData = JsonSerializer.Deserialize<List<object>>(jsonString);
                            return Ok(new { status = "success", JasonResult = deserializedData ?? new List<object>() });
                        }
                        return Ok(new { status = "success", JasonResult = new List<object>() }); // Fallback empty list
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
                Console.WriteLine($"Stack Trace: {ex.StackTrace}");
                return StatusCode(500, new { status = "error", message = ex.Message });
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