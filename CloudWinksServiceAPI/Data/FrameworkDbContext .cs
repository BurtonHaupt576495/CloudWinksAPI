using Microsoft.EntityFrameworkCore;
using CloudWinksServiceAPI.Models;

namespace CloudWinksServiceAPI.Data
{
    public class SpinacheDbContext : DbContext
    {
        public SpinacheDbContext(DbContextOptions<SpinacheDbContext> options) : base(options) { }

        // Define DbSet properties for your tables
        public DbSet<CWAction> CWActions { get; set; }
        public DbSet<CWApp> CWApps { get; set; }
        public DbSet<CWButton> CWButtons { get; set; }
        public DbSet<CWConstant> CWConstants { get; set; }
        public DbSet<CWControl> CWControls { get; set; }
        public DbSet<CWField> CWFields { get; set; }
        public DbSet<CWKey> CWKeys { get; set; }
        public DbSet<CWMenuTabOption> CWMenuTabOptions { get; set; }
        public DbSet<CWParam> CWParams { get; set; }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            // Configure entity keys
            modelBuilder.Entity<CWApp>().HasKey(a => a._appid);
            modelBuilder.Entity<CWAction>().HasKey(a => a.ActionId);
            modelBuilder.Entity<CWButton>().HasKey(a => a.ButtonId);
            modelBuilder.Entity<CWConstant>().HasKey(a => a.ConstantName);
            modelBuilder.Entity<CWControl>().HasKey(a => a.ControlId);
            modelBuilder.Entity<CWField>().HasKey(a => a.FieldId);
            modelBuilder.Entity<CWKey>().HasKey(a => a.KeyId);
            modelBuilder.Entity<CWMenuTabOption>().HasKey(a => a.OptionId);
            modelBuilder.Entity<CWParam>().HasKey(a => a.ParamId);
        }
    }
}
