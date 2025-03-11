namespace CloudWinksServiceAPI.Models
{
    public class CWAction
    {
        public int ActionId { get; set; }
        public int? ControlId { get; set; }
        public string Control { get; set; }
        public string Intent { get; set; }
        public string DataFields { get; set; }
        public string Constants { get; set; }
        public int? IntentId { get; set; }
    }
}
