using System.Runtime.Serialization;

namespace CloudWinksServiceAPI.Models
{
    [DataContract]
    public class ApiResponse<T>
    {
        [DataMember(Name = "status")]
        public string Status { get; set; } // e.g., "Success", "Error"

        [DataMember(Name = "message")]
        public string Message { get; set; }

        [DataMember(Name = "data")]
        public T Data { get; set; }
    }
}
