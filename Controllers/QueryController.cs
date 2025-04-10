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
                            if (connection is NpgsqlConnection)
                            {
                                // PostgreSQL: Use SELECT to call stored function
                                var paramPlaceholders = new List<string>();
                                for (int i = 0; i < request.Parameters.Count; i++)
                                {
                                    paramPlaceholders.Add($"${i + 1}");
                                }

                                string paramList = string.Join(", ", paramPlaceholders);
                                command.CommandText = $"SELECT dbo.\"{request.Name}\"({paramList})";
                                command.CommandType = CommandType.Text;

                                for (int i = 0; i < request.Parameters.Count; i++)
                                {
                                    var param = request.Parameters[i];
                                    var paramValue = ConvertJsonElement(param.Value, param.Type);

                                    var npgParam = new NpgsqlParameter
                                    {
                                        Value = paramValue ?? DBNull.Value
                                    };

                                    SetNpgsqlDbType(npgParam, param.Type);
                                    command.Parameters.Add(npgParam);

                                    Console.WriteLine($"Parameter {i + 1}: Name={param.Name}, Type={param.Type}, Value={paramValue}");
                                }
                            }
                            else
                            {
                                // MSSQL-style stored procedure
                                command.CommandType = CommandType.StoredProcedure;
                                command.CommandText = $"dbo.{request.Name}";

                                for (int i = 0; i < request.Parameters.Count; i++)
                                {
                                    var param = request.Parameters[i];
                                    var paramValue = ConvertJsonElement(param.Value, param.Type);

                                    var dbParam = new NpgsqlParameter
                                    {
                                        ParameterName = $"p{i + 1}",
                                        Value = paramValue ?? DBNull.Value
                                    };

                                    SetNpgsqlDbType(dbParam, param.Type);
                                    command.Parameters.Add(dbParam);
                                }
                            }
                        }
                        else
                        {
                            command.CommandText = $"SELECT * FROM {request.Name}";
                            command.CommandType = CommandType.Text;
                        }

                        object jsonResult;

                        using (var reader = await command.ExecuteReaderAsync())
                        {
                            if (reader.HasRows)
                            {
                                await reader.ReadAsync();
                                jsonResult = reader.GetValue(0);
                            }
                            else
                            {
                                return Ok(new object[0]);
                            }
                        }

                        Console.WriteLine($"Raw JSON result: {jsonResult}");

                        if (jsonResult is string jsonString)
                        {
                            return Ok(JsonSerializer.Deserialize<object>(jsonString));
                        }
                        return Ok(jsonResult ?? new object[0]);
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

        private void SetNpgsqlDbType(NpgsqlParameter parameter, string postgresType)
        {
            switch (postgresType.ToLower())
            {
                case "integer":
                case "int":
                    parameter.NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer;
                    break;
                case "text":
                    parameter.NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text;
                    break;
                case "varchar":
                    parameter.NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Varchar;
                    break;
                case "boolean":
                case "bool":
                    parameter.NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Boolean;
                    break;
                case "double precision":
                case "float":
                    parameter.NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Double;
                    break;
                case "timestamp with time zone":
                    parameter.NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.TimestampTz;
                    break;
                case "bytea":
                    parameter.NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Bytea;
                    break;
                case "smallint":
                    parameter.NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Smallint;
                    break;
                default:
                    break; // Let Npgsql infer the type for unknown cases
            }
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

                    case "bytea":
                        if (jsonElement.ValueKind == JsonValueKind.Null) return null;
                        if (jsonElement.ValueKind == JsonValueKind.String)
                        {
                            string? base64 = jsonElement.GetString();
                            return string.IsNullOrEmpty(base64) ? null : Convert.FromBase64String(base64);
                        }
                        throw new InvalidOperationException($"Cannot convert {jsonElement.ValueKind} to bytea for value: {jsonElement}");

                    default:
                        throw new NotSupportedException($"Unsupported PostgreSQL type: {postgresType}");
                }
            }
            return value;
        }

        public class ParameterInfo
        {
            public string Name { get; set; } = string.Empty;
            public object? Value { get; set; }
            public string Type { get; set; } = "text";
        }

        public class ExecuteRequest
        {
            public int AppId { get; set; }
            public string Name { get; set; } = string.Empty;
            public List<ParameterInfo> Parameters { get; set; } = new List<ParameterInfo>();
        }
    }
}
