using Microsoft.AspNetCore.Mvc;
using Npgsql;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using System.Collections.Generic;
using Dapper;
using System.Linq;

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

                    var isStoredProcedure = await IsStoredProcedure(request.Name, connection);

                    if (isStoredProcedure)
                    {
                        if (request.Parameters != null && request.Parameters.Any())
                        {
                            bool isPostgresFormat = request.Parameters.Any(p =>
                                p.Value is Dictionary<string, object> dict &&
                                dict.ContainsKey("type") && dict.ContainsKey("value"));

                            if (isPostgresFormat)
                            {
                                var parameters = new DynamicParameters();
                                int position = 0;
                                foreach (var param in request.Parameters)
                                {
                                    var paramData = param.Value as Dictionary<string, object>;
                                    if (paramData != null)
                                    {
                                        string paramType = paramData["type"]?.ToString() ?? "text";
                                        var paramValue = ConvertJsonElement(paramData["value"], paramType);
                                        parameters.Add($"p{++position}", paramValue);
                                    }
                                }

                                var result = await connection.QuerySingleOrDefaultAsync<string>(
                                    $"SELECT dbo.\"{request.Name}\"({string.Join(", ", Enumerable.Range(1, parameters.ParameterNames.Count()).Select(i => $"@p{i}"))})");

                                return Ok(JsonSerializer.Deserialize<object>(result));
                            }
                        }

                                string query = $"SELECT dbo.\"{request.Name}\"({string.Join(", ", Enumerable.Range(1, parameters.ParameterNames.Count()).Select(i => $"@p{i}"))})";
                                var result = await connection.QuerySingleOrDefaultAsync<string>(query, parameters);
                                Console.WriteLine($"Raw JSON result: {result}");

                                if (string.IsNullOrEmpty(result))
                                {
                                    return Ok(new object[0]);
                                }
                                return Ok(JsonSerializer.Deserialize<object>(result));
                            }
                            else
                            {
                                // MSSQL case
                                var paramString = string.Join(", ", request.Parameters.Select(p => $"@{p.Key}"));
                                var parameters = new DynamicParameters();
                                foreach (var param in request.Parameters)
                                {
                                    var value = ConvertJsonElement(param.Value, null);
                                    Console.WriteLine($"Parameter {param.Key}: {value}");
                                    parameters.Add(param.Key, value ?? DBNull.Value);
                                }
                                string query = $"SELECT dbo.\"{request.Name}\"({paramString})";
                                var jsonResult = await connection.ExecuteScalarAsync<string>(query, parameters);
                                Console.WriteLine($"Raw JSON result: {jsonResult}");

                                if (!string.IsNullOrEmpty(jsonResult))
                                {
                                    return Ok(JsonSerializer.Deserialize<object>(jsonResult));
                                }
                                return Ok(new object[0]);
                            }
                        }
                        else
                        {
                            string query = $"SELECT dbo.\"{request.Name}\"()";
                            var jsonResult = await connection.ExecuteScalarAsync<string>(query);
                            Console.WriteLine($"Raw JSON result: {jsonResult}");

                            if (!string.IsNullOrEmpty(jsonResult))
                            {
                                return Ok(JsonSerializer.Deserialize<object>(jsonResult));
                            }
                            return Ok(new object[0]);
                        }
                    }
                    else
                    {
                        string query = $"SELECT * FROM {request.Name}";
                        var jsonResult = await connection.ExecuteScalarAsync<string>(query);
                        Console.WriteLine($"Raw JSON result: {jsonResult}");

                        if (!string.IsNullOrEmpty(jsonResult))
                        {
                            return Ok(JsonSerializer.Deserialize<object>(jsonResult));
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

        private async Task<bool> IsStoredProcedure(string name, NpgsqlConnection connection)
        {
            var query = "SELECT COUNT(*) FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid WHERE n.nspname = 'dbo' AND p.proname = LOWER(@name)";
            var count = await connection.ExecuteScalarAsync<long>(query, new { name = name.ToLower() });
            return count > 0;
        }

        private object? ConvertJsonElement(object? value, string? postgresType)
        {
            if (value is JsonElement jsonElement)
            {
                if (string.IsNullOrEmpty(postgresType))
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

                switch (postgresType.ToLower())
                {
                    case "integer":
                    case "int":
                        if (jsonElement.ValueKind == JsonValueKind.Null) return null;
                        if (jsonElement.ValueKind == JsonValueKind.Number && jsonElement.TryGetInt32(out int intValue)) return intValue;
                        if (jsonElement.ValueKind == JsonValueKind.String && int.TryParse(jsonElement.GetString(), out int parsedInt)) return parsedInt;
                        throw new InvalidOperationException($"Cannot convert {jsonElement.ValueKind} to integer for value: {jsonElement}");

                    case "text":
                    case "varchar":
                        if (jsonElement.ValueKind == JsonValueKind.Null) return null;
                        if (jsonElement.ValueKind == JsonValueKind.String) return jsonElement.GetString();
                        return jsonElement.ToString();

                    case "boolean":
                    case "bool":
                        if (jsonElement.ValueKind == JsonValueKind.Null) return null;
                        if (jsonElement.ValueKind == JsonValueKind.True) return true;
                        if (jsonElement.ValueKind == JsonValueKind.False) return false;
                        if (jsonElement.ValueKind == JsonValueKind.String && bool.TryParse(jsonElement.GetString(), out bool boolValue)) return boolValue;
                        throw new InvalidOperationException($"Cannot convert {jsonElement.ValueKind} to boolean for value: {jsonElement}");

                    case "double precision":
                    case "float":
                        if (jsonElement.ValueKind == JsonValueKind.Null) return null;
                        if (jsonElement.ValueKind == JsonValueKind.Number && jsonElement.TryGetDouble(out double doubleValue)) return doubleValue;
                        if (jsonElement.ValueKind == JsonValueKind.String && double.TryParse(jsonElement.GetString(), out double parsedDouble)) return parsedDouble;
                        throw new InvalidOperationException($"Cannot convert {jsonElement.ValueKind} to double for value: {jsonElement}");

                    default:
                        throw new NotSupportedException($"Unsupported PostgreSQL type: {postgresType}");
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