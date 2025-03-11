namespace CloudWinksServiceAPI.Models
{
    public class CWParam
    {
        public int ParamId { get; set; }
        public int? ControlId { get; set; }
        public string Control { get; set; }
        public int? Seq { get; set; }
        public string Name { get; set; }
        public int? DbType { get; set; }
        public string DbTypeName { get; set; }
        public string DefaultValue { get; set; }
    }
}
