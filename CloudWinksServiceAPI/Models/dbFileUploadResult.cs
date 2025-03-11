using System.Runtime.Serialization;

namespace CloudWinksServiceAPI.Models
{
    [DataContract]
    public class dbFileUploadResult
    {
        [DataMember(Name = "fileName")]
        public string FileName { get; set; }

        [DataMember(Name = "fileSize")]
        public long FileSize { get; set; }

        [DataMember(Name = "message")]
        public string Message { get; set; }
    }
}
