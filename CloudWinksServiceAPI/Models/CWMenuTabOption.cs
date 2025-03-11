namespace CloudWinksServiceAPI.Models
{
    public class CWMenuTabOption
    {
        public int OptionId { get; set; }
        public int? ControlId { get; set; }
        public string Control { get; set; }
        public int? Seq { get; set; }
        public string TargetIntent { get; set; }
        public string TargetTable { get; set; }
        public int? TargetMode { get; set; }
        public string Mode { get; set; }
        public int? TargetDataSourceId { get; set; }
        public string DataSource { get; set; }
        public string ImageId { get; set; }
        public string Description { get; set; }
        public string TitleImage { get; set; }
        public int? TargetIntentId { get; set; }
        public int? TargetTableId { get; set; }
    }
}
