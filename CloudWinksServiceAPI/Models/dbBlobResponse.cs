using System.Runtime.Serialization;

namespace CloudWinksServiceAPI.Models
{
    [DataContract]
    public class BlobResponse
    {
        [DataMember(Name = "contentType")]
        public string ContentType { get; set; }

        [DataMember(Name = "data")]
        public byte[] Data { get; set; }
    }
}
