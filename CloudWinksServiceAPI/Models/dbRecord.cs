using System.Collections.Generic;
using System.Runtime.Serialization;

namespace CloudWinksServiceAPI.Models
{
    [DataContract]
    public class dbRecord
    {
        [DataMember(Name = "fields")]
        public List<dbField> Fields { get; set; }
    }
}
