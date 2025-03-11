namespace CloudWinksServiceAPI.Models
{
    public class CWControl
    {
        public int ControlId { get; set; }
        public string Name { get; set; }
        public bool TitleBar { get; set; }
        public string Title { get; set; }
        public string TitleField { get; set; }
        public string TitleImageField { get; set; }
        public string Layout { get; set; }
        public bool Context { get; set; }
        public int? Mode { get; set; }
        public string ModeName { get; set; }
        public int? MenuType { get; set; }
        public string MenuTypeName { get; set; }
        public int? GridColumns { get; set; }
        public bool PickContact { get; set; }
        public bool GetGeo { get; set; }
        public string TargetIntent { get; set; }
        public string TargetTable { get; set; }
        public int? TargetMode { get; set; }
        public string TargetModeName { get; set; }
        public string TargetIntentField { get; set; }
        public string TargetTableField { get; set; }
        public string TargetModeField { get; set; }
        public string TargetDataSourceField { get; set; }
        public int? AppId { get; set; }
        public string DataSource { get; set; }
        public string TargetDataSource { get; set; }
        public int? TargetIntentId { get; set; }
        public int? TargetTableId { get; set; }
        public int? TargetDataSourceId { get; set; }
        public int? ControlType { get; set; }
        public int? FormatId { get; set; }
        public string FormatName { get; set; }
    }
}
