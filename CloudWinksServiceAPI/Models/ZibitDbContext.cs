using Microsoft.EntityFrameworkCore;

public class ZibitDbContext : DbContext
{
    public ZibitDbContext(DbContextOptions<ZibitDbContext> options) : base(options)
    {
    }

    // You don't need to define DbSets since you're using a stored procedure
}
