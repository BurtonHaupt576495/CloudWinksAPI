using System.Collections.Generic;
using System.Runtime.Serialization;

namespace CloudWinksServiceAPI.Models
{
    [DataContract]
    public class dbQueryResult
    {
        [DataMember(Name = "records")]
        public List<dbRecord> Records { get; set; }

        [DataMember(Name = "tableName")]
        public string TableName { get; set; }

        [DataMember(Name = "rowCount")]
        public int RowCount { get; set; }
    }
}
