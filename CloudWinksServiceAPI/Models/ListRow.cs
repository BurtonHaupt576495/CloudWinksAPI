using System.Collections.Generic;
using System.Runtime.Serialization;

namespace CloudWinksServiceAPI.Models
{
    [DataContract]
    public class ListRow
    {
        [DataMember(Name = "fields")]
        public List<ListField> Fields { get; set; } = new List<ListField>();
    }
}
