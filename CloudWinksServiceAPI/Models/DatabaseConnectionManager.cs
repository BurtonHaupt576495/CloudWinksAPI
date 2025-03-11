using System;
using System.Collections.Concurrent;
using System.Data.SqlClient;

public class DatabaseConnectionManager
{
    private readonly ConcurrentDictionary<int, string> _connectionStrings = new ConcurrentDictionary<int, string>();
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

            return new SqlConnectionStringBuilder
            {
                UserID = config.UserId,
                Password = config.Password,
                InitialCatalog = config.Database,
                DataSource = config.Server,
                MultipleActiveResultSets = true,
                TrustServerCertificate = true
            }.ConnectionString;
        });
    }

    private (string UserId, string Password, string Database, string Server) FetchAppConfig(int appId)
    {
        using (var connection = new SqlConnection(_defaultConnectionString))
        {
            connection.Open();

            const string query = "SELECT _userId, _userPassword, _userDatabase, _server FROM dbo.Applications WHERE _appid = @appId";
            using (var command = new SqlCommand(query, connection))
            {
                command.Parameters.AddWithValue("@appId", appId);

                using (var reader = command.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        return (
                            UserId: reader["_userId"].ToString(),
                            Password: reader["_userPassword"].ToString(),
                            Database: reader["_userDatabase"].ToString(),
                            Server: reader["_server"].ToString()
                        );
                    }
                    else
                    {
                        throw new Exception($"App with ID {appId} not found in dbo.Applications.");
                    }
                }
            }
        }
    }
}
