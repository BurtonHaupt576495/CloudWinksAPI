using CloudWinksServiceAPI.Interfaces;
using CloudWinksServiceAPI.Models;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
namespace CloudWinksServiceAPI.Services
{
    public class DynamicQueryService : IDynamicQueryService
    {
        private readonly string _connectionString;

        public DynamicQueryService(IConfiguration configuration)
        {
            _connectionString = configuration.GetConnectionString("DefaultConnection");
        }

        public async Task<dbQueryResult> ExecuteQueryWithParametersAsync(string tableName, dbFields parameters)
        {
            using (var connection = new SqlConnection(_connectionString))
            {
                var query = $"SELECT * FROM {tableName} WHERE {BuildWhereClause(parameters)}";
                var dbParams = MapToSqlParameters(parameters);

                try
                {
                    var result = await connection.QueryAsync(query, dbParams);

                    return new dbQueryResult
                    {
                        TableName = tableName,
                        Records = result.Select(MapToDbRecord).ToList(),
                        RowCount = result.Count()
                    };
                }
                catch (Exception ex)
                {
                    // Log the exception and rethrow or handle as necessary
                    throw new Exception("Error executing query", ex);
                }
            }
        }

        public async Task<dbQueryResult> ExecuteQueryWithoutParametersAsync(string tableName)
        {
            using (var connection = new SqlConnection(_connectionString))
            {
                var query = $"SELECT * FROM {tableName}";

                try
                {
                    // Execute the query
                    var result = await connection.QueryAsync<dynamic>(query); // Explicitly use dynamic

                    // Check if the result is null or empty
                    if (result == null || !result.Any())
                    {
                        return new dbQueryResult
                        {
                            TableName = tableName,
                            Records = new List<dbRecord>(),
                            RowCount = 0
                        };
                    }

                    // Map each dynamic row to dbRecord
                    var mappedRecords = new List<dbRecord>();

                    foreach (var row in result)
                    {
                        mappedRecords.Add(MapToDbRecord(row));
                    }


                    return new dbQueryResult
                    {
                        TableName = tableName,
                        Records = mappedRecords,
                        RowCount = mappedRecords.Count
                    };
                }
                catch (Exception ex)
                {
                    throw new Exception($"Error executing query on table {tableName} without parameters.", ex);
                }
            }
        }

        public async Task<ListQueryResult> ExecuteListQueryAsync(string tableName)
        {
            using (var connection = new SqlConnection(_connectionString))
            {
                var query = $"SELECT * FROM {tableName}";

                try
                {
                    var result = await connection.QueryAsync(query);

                    if (result == null || !result.Any())
                    {
                        return new ListQueryResult
                        {
                            TableName = tableName,
                            Listrows = new List<ListRow>(),
                            RowCount = 0
                        };
                    }

                    // Log or inspect each row for debugging
                    foreach (var row in result)
                    {
                        Console.WriteLine(row == null ? "Null Row" : "Valid Row");
                    }

                    return new ListQueryResult
                    {
                        TableName = tableName,
                        Listrows = result.Select(MapToListRow).ToList(),
                        RowCount = result.Count()
                    };
                }
                catch (Exception ex)
                {
                    throw new Exception("Error executing query without parameters", ex);
                }
            }
        }

        public async Task<int> ExecuteNonQueryAsync(string queryName, dbFields parameters)
        {
            using (var connection = new SqlConnection(_connectionString))
            {
                try
                {
                    var dbParams = MapToSqlParameters(parameters);

                    return await connection.ExecuteAsync(queryName, dbParams, commandType: CommandType.StoredProcedure);
                }
                catch (Exception ex)
                {
                    // Log the exception and rethrow or handle as necessary
                    throw new Exception("Error executing non-query", ex);
                }
            }
        }

        public async Task<int> ExecuteNonQueryAsync(string queryName)
        {
            using (var connection = new SqlConnection(_connectionString))
            {
                try
                {
                    // Execute the stored procedure without parameters
                    return await connection.ExecuteAsync(queryName, commandType: CommandType.StoredProcedure);
                }
                catch (Exception ex)
                {
                    // Log the exception and rethrow or handle as necessary
                    throw new Exception("Error executing non-query", ex);
                }
            }
        }

        public async Task<List<dbRecord>> ExecuteStoredProcedureAsync(string procedureName)
        {
            using (var connection = new SqlConnection(_connectionString))
            {
                try
                {
                    // Execute the stored procedure
                    var result = await connection.QueryAsync<dynamic>($"EXEC {procedureName}");

                    // Map the result (IEnumerable<dynamic>) to List<dbRecord>
                    var mappedResult = result.Select(row => MapToDbRecord((IDictionary<string, object>)row)).ToList();

                    return mappedResult;
                }
                catch (Exception ex)
                {
                    throw new Exception($"Error executing stored procedure {procedureName}", ex);
                }
            }
        }

        //new
        public async Task<dbQueryResult> ExecuteStoredProcedureWithParametersAsync(string procedureName, dbFields parameters)
        {
            using (var connection = new SqlConnection(_connectionString))
            {
                try
                {
                    var dbParams = MapToSqlParameters(parameters);

                    Console.WriteLine($"Executing stored procedure: {procedureName}");
                    foreach (var param in parameters)
                    {
                        Console.WriteLine($"Param Name: {param.Name}, Value: {param.Value}");
                    }

                    var result = await connection.QueryAsync($"EXEC {procedureName} @controlid", dbParams);

                    Console.WriteLine($"Result Count: {result.Count()}");
                    foreach (var row in result)
                    {
                        Console.WriteLine(row ?? "Null Row");
                    }

                    if (result == null || !result.Any())
                    {
                        return new dbQueryResult
                        {
                            TableName = procedureName,
                            Records = new List<dbRecord>(),
                            RowCount = 0
                        };
                    }

                    return new dbQueryResult
                    {
                        TableName = procedureName,
                        Records = result.Select(MapToDbRecord).ToList(),
                        RowCount = result.Count()
                    };
                }
                catch (Exception ex)
                {
                    throw new Exception($"Error executing stored procedure {procedureName}", ex);
                }
            }
        }

        public async Task<dbQueryResult> ExecuteStoredProcedureForUsersParametersAsync(string procedureName, dbFields parameters)
        {
            using (var connection = new SqlConnection(_connectionString))
            {
                try
                {
                    var dbParams = MapToSqlParameters(parameters);

                    Console.WriteLine($"Executing stored procedure: {procedureName}");
                    foreach (var param in parameters)
                    {
                        Console.WriteLine($"Param Name: {param.Name}, Value: {param.Value}");
                    }

                    var query = $"EXEC {procedureName} " + string.Join(", ", parameters.Select(p => p.Name));
                    var result = await connection.QueryAsync(query, dbParams);

                    Console.WriteLine($"Result Count: {result.Count()}");
                    foreach (var row in result)
                    {
                        Console.WriteLine(row ?? "Null Row");
                    }

                    if (result == null || !result.Any())
                    {
                        return new dbQueryResult
                        {
                            TableName = procedureName,
                            Records = new List<dbRecord>(),
                            RowCount = 0
                        };
                    }

                    return new dbQueryResult
                    {
                        TableName = procedureName,
                        Records = result.Select(MapToDbRecord).ToList(),
                        RowCount = result.Count()
                    };
                }
                catch (Exception ex)
                {
                    throw new Exception($"Error executing stored procedure {procedureName}", ex);
                }
            }
        }

        private static string BuildWhereClause(dbFields parameters)
        {
            return string.Join(" AND ", parameters.Select(p => $"{p.Name} = @{p.Name}"));
        }

        //private static DynamicParameters MapToSqlParameters(dbFields fields)
        //{
        //    var dbParams = new DynamicParameters();
        //    foreach (var field in fields)
        //    {
        //        dbParams.Add($"@{field.Name}", field.Value, MapDbType(field.Type));
        //    }
        //    return dbParams;
        //}

        private static DynamicParameters MapToSqlParameters(dbFields fields)
        {
            var dbParams = new DynamicParameters();

            foreach (var field in fields)
            {
                object value;

                // Handle JsonElement conversion
                if (field.Value is JsonElement jsonElement)
                {
                    // Convert JsonElement to its appropriate .NET type
                    switch (jsonElement.ValueKind)
                    {
                        case JsonValueKind.Number:
                            value = jsonElement.TryGetInt32(out var intValue) ? intValue : jsonElement.GetDecimal();
                            break;
                        case JsonValueKind.String:
                            value = jsonElement.GetString();
                            break;
                        case JsonValueKind.True:
                        case JsonValueKind.False:
                            value = jsonElement.GetBoolean();
                            break;
                        case JsonValueKind.Null:
                            value = DBNull.Value;
                            break;
                        default:
                            throw new ArgumentException($"Unsupported JsonElement type: {jsonElement.ValueKind}");
                    }
                }
                else
                {
                    value = field.Value ?? DBNull.Value; // Handle other .NET types or null
                }

                try
                {
                    // Log parameter details for debugging
                    Console.WriteLine($"Adding parameter: @{field.Name}, Value: {value}, Type: {field.Type}");

                    dbParams.Add($"@{field.Name}", value, MapDbType(field.Type));
                }
                catch (Exception ex)
                {
                    throw new Exception($"Error adding parameter @{field.Name}. Details: {ex.Message}", ex);
                }
            }

            return dbParams;
        }

        private static DbType MapDbType(string type)
        {
            if (string.IsNullOrWhiteSpace(type))
            {
                // Handle null or empty type: Defaulting to DbType.String
                return DbType.String;
            }
            return type.ToUpper() switch
            {
                "INTEGER" => DbType.Int32,
                "STRING" => DbType.String,
                "FLOAT" => DbType.Double,
                "BOOLEAN" => DbType.Boolean,
                "DATETIME" => DbType.DateTime,
                _ => DbType.String
            };
        }

        //private static DbType MapDbType(DBTYPE type)
        //{
        //    return type switch
        //    {
        //        DBTYPE.INTEGER => DbType.Int32,
        //        DBTYPE.FLOAT => DbType.Double,
        //        DBTYPE.STRING => DbType.String,
        //        DBTYPE.BOOLEAN => DbType.Boolean,
        //        DBTYPE.DATETIME => DbType.DateTime,
        //        DBTYPE.BLOB => DbType.Binary,
        //        _ => DbType.String
        //    };
        //}


        //allow null values
        public dbRecord MapToDbRecord(dynamic row)
        {
            if (row == null) // Check if the entire row is null
            {
                Console.WriteLine("Row is null.");
                return new dbRecord { Fields = new List<dbField>() };
            }

            Console.WriteLine("Processing row...");

            var fields = new List<dbField>();

            try
            {
                // First, attempt to cast the row to an IDictionary<string, object>
                if (row is IDictionary<string, object> dictionary)
                {
                    foreach (var kvp in dictionary)
                    {
                        fields.Add(new dbField
                        {
                            Name = kvp.Key,
                            Value = kvp.Value ?? string.Empty, // Handle null values by substituting with an empty string
                            Type = InferDbType(kvp.Value?.GetType() ?? typeof(string)).ToString() // Convert enum to string
                        });
                    }
                }
                else
                {
                    // If casting to IDictionary fails, use reflection as a fallback
                    Console.WriteLine("Row is not a dictionary. Attempting to use reflection...");

                    foreach (var property in row.GetType().GetProperties())
                    {
                        fields.Add(new dbField
                        {
                            Name = property.Name,
                            Value = property.GetValue(row) ?? string.Empty, // Handle null values
                            Type = InferDbType(property.PropertyType).ToString() // Convert enum to string
                        });
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error processing row: {ex.Message}");
            }

            return new dbRecord { Fields = fields };
        }


        //private static dbRecord MapToDbRecord(dynamic row)
        //{
        //    var fields = new List<dbField>();

        //    foreach (var property in row.GetType().GetProperties())
        //    {
        //        fields.Add(new dbField
        //        {
        //            Name = property.Name,
        //            Value = property.GetValue(row),
        //            Type = InferDbType(property.PropertyType)
        //        });
        //    }

        //    return new dbRecord { Fields = fields };
        //}

        private static ListRow MapToListRow(dynamic row)
        {
            if (row == null)
            {
                return new ListRow
                {
                    Fields = new List<ListField>()
                };
            }

            var fields = new List<ListField>();

            foreach (var property in row.GetType().GetProperties())
            {
                fields.Add(new ListField
                {
                    Name = property.Name,
                    Value = property.GetValue(row, null)
                });
            }

            return new ListRow { Fields = fields };
        }


        private static DBTYPE InferDbType(Type type)
        {
            if (type == typeof(int)) return DBTYPE.INTEGER;
            if (type == typeof(float) || type == typeof(double)) return DBTYPE.FLOAT;
            if (type == typeof(bool)) return DBTYPE.BOOLEAN;
            if (type == typeof(DateTime)) return DBTYPE.DATETIME;
            if (type == typeof(byte[])) return DBTYPE.BLOB;
            return DBTYPE.STRING;
        }
    }

}
