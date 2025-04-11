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
                            command.CommandType = CommandType.Text;
                            var orderedParams = request.Parameters.ToList();

                            var paramPlaceholders = new List<string>();
                            for (int i = 0; i < orderedParams.Count; i++)
                            {
                                paramPlaceholders.Add($"${i + 1}");
                            }

                            string paramList = string.Join(", ", paramPlaceholders);
                            command.CommandText = $"SELECT * FROM dbo.\"{request.Name}\"({paramList})";

                            for (int i = 0; i < orderedParams.Count; i++)
                            {
                                var param = orderedParams[i];
                                var paramValue = ConvertJsonElement(param.Value, param.Type);

                                var npgParam = new NpgsqlParameter
                                {
                                    Value = paramValue ?? DBNull.Value
                                };
                                SetNpgsqlDbType(npgParam, param.Type);
                                command.Parameters.Add(npgParam);

                                Console.WriteLine($"Parameter {i + 1}: Name={param.Name}, Type={param.Type}, Value={paramValue}");
                            }

                            using (var reader = await command.ExecuteReaderAsync())
                            {
                                var resultList = new List<object>();
                                while (await reader.ReadAsync())
                                {
                                    if (reader.GetFieldType(0) == typeof(string) && reader.GetString(0).StartsWith("{"))
                                    {
                                        return Ok(JsonSerializer.Deserialize<object>(reader.GetString(0)));
                                    }
                                    else
                                    {
                                        var rowDict = new Dictionary<string, object>();
                                        for (int i = 0; i < reader.FieldCount; i++)
                                        {
                                            if (!reader.IsDBNull(i))
                                            {
                                                rowDict[reader.GetName(i)] = reader.GetValue(i);
                                            }
                                        }
                                        resultList.Add(rowDict);
                                    }
                                }
                                return Ok(resultList);
                            }
                        }
                        else
                        {
                            command.CommandText = $"SELECT * FROM {request.Name}";
                            command.CommandType = CommandType.Text;

                            using (var reader = await command.ExecuteReaderAsync())
                            {
                                if (reader.HasRows)
                                {
                                    await reader.ReadAsync();
                                    var jsonResult = reader.GetValue(0);
                                    Console.WriteLine($"Raw JSON result: {jsonResult}");
                                    if (jsonResult is string jsonString)
                                    {
                                        return Ok(JsonSerializer.Deserialize<object>(jsonString));
                                    }
                                    return Ok(jsonResult ?? new object[0]);
                                }
                                return Ok(new object[0]);
                            }
                        }
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
                case "int": parameter.NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer; break;
                case "smallint":
                case "int2": parameter.NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Smallint; break;
                case "bigint": parameter.NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Bigint; break;
                case "numeric": parameter.NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Numeric; break;
                case "real": parameter.NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Real; break;
                case "double precision":
                case "float": parameter.NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Double; break;
                case "boolean":
                case "bool": parameter.NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Boolean; break;
                case "text": parameter.NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text; break;
                case "varchar": parameter.NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Varchar; break;
                case "char": parameter.NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Char; break;
                case "timestamp": parameter.NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp; break;
                case "timestamp with time zone":
                case "timestamptz": parameter.NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.TimestampTz; break;
                case "date": parameter.NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Date; break;
                case "time": parameter.NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Time; break;
                case "uuid": parameter.NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Uuid; break;
                case "json": parameter.NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Json; break;
                case "jsonb": parameter.NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Jsonb; break;
                case "bytea": parameter.NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Bytea; break;
                default: break; // Let Npgsql infer
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
                            if (jsonElement.TryGetInt32(out int i)) return i;
                            if (jsonElement.TryGetDouble(out double d)) return d;
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
                        return null; // Fallback instead of exception

                    case "text":
                    case "varchar":
                        return jsonElement.ValueKind == JsonValueKind.Null ? null : jsonElement.GetString();

                    case "boolean":
                    case "bool":
                        return jsonElement.ValueKind switch
                        {
                            JsonValueKind.True => true,
                            JsonValueKind.False => false,
                            JsonValueKind.String => bool.TryParse(jsonElement.GetString(), out bool b) ? b : null,
                            JsonValueKind.Null => null,
                            _ => null
                        };

                    case "double precision":
                    case "float":
                        return jsonElement.ValueKind == JsonValueKind.Null ? null : jsonElement.GetDouble();

                    case "timestamp":
                    case "timestamptz":
                    case "timestamp with time zone":
                        if (jsonElement.ValueKind == JsonValueKind.Null) return null;
                        if (jsonElement.ValueKind == JsonValueKind.String && DateTime.TryParse(jsonElement.GetString(), out var dt))
                            return DateTime.SpecifyKind(dt.ToUniversalTime(), DateTimeKind.Utc);
                        return null;

                    case "smallint":
                    case "int2":
                        if (jsonElement.ValueKind == JsonValueKind.Null) return null;
                        if (jsonElement.ValueKind == JsonValueKind.Number && jsonElement.TryGetInt16(out var shortValue))
                            return shortValue;
                        return null;

                    case "date":
                        return jsonElement.ValueKind == JsonValueKind.Null ? null : jsonElement.GetDateTime().Date;

                    case "time":
                        return jsonElement.ValueKind == JsonValueKind.Null ? null : jsonElement.GetDateTime().TimeOfDay;

                    case "bytea":
                        if (jsonElement.ValueKind == JsonValueKind.Null) return null;
                        if (jsonElement.ValueKind == JsonValueKind.String)
                        {
                            string? base64 = jsonElement.GetString();
                            if (string.IsNullOrWhiteSpace(base64)) return null;
                            try
                            {
                                return Convert.FromBase64String(base64);
                            }
                            catch (FormatException)
                            {
                                Console.WriteLine($"⚠️ Invalid Base64 string received for bytea field: {base64}");
                                return null;
                            }
                        }
                        return null;

                    default:
                        return null; // Fallback instead of exception
                }
            }

            // Handle deserialized values (e.g., int from ASP.NET Core)
            switch (postgresType?.ToLower())
            {
                case "smallint":
                case "int2":
                    if (value is int intValue && intValue >= -32768 && intValue <= 32767) return (short)intValue;
                    return value; // Fallback to original value if out of range
                default:
                    return value; // Return as-is for other types
            }
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
            public List<ParameterInfo> Parameters { get; set; } = new();
        }
    }
}
