using System.Collections.Generic;
using System.Runtime.Serialization;

namespace CloudWinksServiceAPI.Models
{
    [DataContract]
    public class ListQueryResult
    {
        [DataMember(Name = "listrows")]
        public List<ListRow> Listrows { get; set; } = new List<ListRow>();

        [DataMember(Name = "tableName")]
        public string TableName { get; set; }

        [DataMember(Name = "rowCount")]
        public int RowCount { get; set; }
    }
}
