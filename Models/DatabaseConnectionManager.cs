using System;
using System.Collections.Concurrent;
using Npgsql;

public class DatabaseConnectionManager
{
    private readonly ConcurrentDictionary<int, string> _connectionStrings = new();
    private readonly string _defaultConnectionString;

    public DatabaseConnectionManager(string defaultConnectionString)
    {
        _defaultConnectionString = defaultConnectionString ?? throw new ArgumentNullException(nameof(defaultConnectionString));
    }

    public string GetOrAddConnectionString(int appId)
    {
        return _connectionStrings.GetOrAdd(appId, id =>
        {
            var config = FetchAppConfig(id);

            return new NpgsqlConnectionStringBuilder
            {
                Host = config.Server,
                Database = config.Database,
                Username = config.UserId,
                Password = config.Password,
                Port = 5432,
                Pooling = true
            }.ConnectionString;
        });
    }

    private (string UserId, string Password, string Database, string Server) FetchAppConfig(int appId)
    {
        using (var connection = new NpgsqlConnection(_defaultConnectionString))
        {
            connection.Open();

            const string query = "SELECT _userId, _userPassword, _userDatabase, _server FROM applications WHERE _appid = @appId";
            using (var command = new NpgsqlCommand(query, connection))
            {
                command.Parameters.AddWithValue("@appId", appId);

                using (var reader = command.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        return (
                            UserId: reader["_userId"].ToString()!,
                            Password: reader["_userPassword"].ToString()!,
                            Database: reader["_userDatabase"].ToString()!,
                            Server: reader["_server"].ToString()!
                        );
                    }
                    else
                    {
                        throw new Exception($"App with ID {appId} not found in applications.");
                    }
                }
            }
        }
    }
}