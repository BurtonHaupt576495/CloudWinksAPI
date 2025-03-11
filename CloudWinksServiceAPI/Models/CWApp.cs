namespace CloudWinksServiceAPI.Models
{
    public class CWApp
    {
        
        public int _appid { get; set; }
        public string _appname { get; set; }
        public int? _targettableid { get; set; }
        public int? _targetmode { get; set; }
        public int? _targetdatasourceid { get; set; }
        public int? _applogoid { get; set; }
        public int? _titleimageid { get; set; }
        public int _apptype { get; set; }
        public int? _seq { get; set; }
    }
}