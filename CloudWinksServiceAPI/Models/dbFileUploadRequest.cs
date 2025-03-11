using System.Runtime.Serialization;

namespace CloudWinksServiceAPI.Models
{
    [DataContract]
    public class dbFileUploadRequest
    {
        [DataMember(Name = "fileName")]
        public string FileName { get; set; }

        [DataMember(Name = "contentType")]
        public string ContentType { get; set; }

        [DataMember(Name = "data")]
        public byte[] Data { get; set; }
    }
}
