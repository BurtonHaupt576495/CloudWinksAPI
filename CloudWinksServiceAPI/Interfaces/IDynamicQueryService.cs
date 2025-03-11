using CloudWinksServiceAPI.Models;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace CloudWinksServiceAPI.Interfaces
{
    public interface IDynamicQueryService
    {
        // Execute queries with parameters
        Task<dbQueryResult> ExecuteQueryWithParametersAsync(string tableName, dbFields parameters);


        // Execute queries without parameters
        Task<dbQueryResult> ExecuteQueryWithoutParametersAsync(string tableName);


        // Execute list queries
        Task<ListQueryResult> ExecuteListQueryAsync(string tableName);



        // Execute stored procedures with parameters and return results
        Task<dbQueryResult> ExecuteStoredProcedureWithParametersAsync(string procedureName, dbFields parameters);


        // Execute non-query stored procedures with parameters
        Task<int> ExecuteNonQueryAsync(string queryName, dbFields parameters);


        // Execute non-query stored procedures without parameters
        Task<int> ExecuteNonQueryAsync(string queryName);
        // Task<dbQueryResult> ExecuteNonQueryAsync(string queryName);


        // Execute stored procedures without parameters and return a list of records
        Task<List<dbRecord>> ExecuteStoredProcedureAsync(string procedureName);

        // Task<List<string>> ExecuteStoredProcedureAsync(string procedureName);

        //Users
        Task<dbQueryResult> ExecuteStoredProcedureForUsersParametersAsync(string procedureName, dbFields parameters);

        // Map dynamic rows to dbRecords (added as public for controller access)
        dbRecord MapToDbRecord(dynamic row);
    }
}