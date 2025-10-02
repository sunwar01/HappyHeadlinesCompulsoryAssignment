using ArticleService.Models;
using Microsoft.EntityFrameworkCore;

namespace ArticleService.Data;

public class ArticleDbContext : DbContext
{
    public DbSet<Article> Articles => Set<Article>();


    public ArticleDbContext(DbContextOptions<ArticleDbContext> options) : base(options) { }


    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Article>(e =>
        {
            e.HasKey(a => a.Id);
            e.Property(a => a.Title).IsRequired().HasMaxLength(200);
            e.Property(a => a.Continent).IsRequired().HasMaxLength(16);
            e.HasIndex(a => a.PublishedAt);
            e.HasIndex(a => new { a.Continent, a.PublishedAt });
        });
    }
}