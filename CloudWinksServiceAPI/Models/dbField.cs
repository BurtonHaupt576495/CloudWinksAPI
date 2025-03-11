using System.Runtime.Serialization;

namespace CloudWinksServiceAPI.Models
{
    [DataContract]
    public class dbField
    {

        public string Name { get; set; }
        public object Value { get; set; }
        public string Type { get; set; }
    }

    public enum DBTYPE
    {
        INTEGER,
        STRING,
        FLOAT,
        BOOLEAN,
        DATETIME,
        TIME,
        BLOB,
        MONEY
    }
}
