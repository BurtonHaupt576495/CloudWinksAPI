namespace CloudWinksServiceAPI.Models
{
    public class CWButton
    {
        public int ButtonId { get; set; }
        public int? ControlId { get; set; }
        public string Control { get; set; }
        public string Text { get; set; }
        public int? Group { get; set; }
        public int? Id { get; set; }
        public int? Seq { get; set; }
        public int? Action { get; set; }
        public string Act { get; set; }
        public string Key { get; set; }
        public int? TargetIntentId { get; set; }
        public int? TargetTableId { get; set; }
        public int? TargetDataSourceId { get; set; }
        public int? TargetModeId { get; set; }
    }
}
