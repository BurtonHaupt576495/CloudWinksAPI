namespace CloudWinksServiceAPI.Models
{
    public class CWField
    {
        public int FieldId { get; set; }
        public int? ControlId { get; set; }
        public string Control { get; set; }
        public int? Seq { get; set; }
        public string Id { get; set; }
        public string Label { get; set; }
        public int? DbType { get; set; }
        public string DbTypeName { get; set; }
        public int? InputType { get; set; }
        public string InputTypeName { get; set; }
        public bool Visible { get; set; }
        public bool Enabled { get; set; }
        public int? Alignment { get; set; }
        public string AlignmentName { get; set; }
        public string DefaultImage { get; set; }
        public string DefaultTextField { get; set; }
    }
}
