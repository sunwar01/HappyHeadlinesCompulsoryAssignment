using ArticleService.Models;
using Microsoft.EntityFrameworkCore;

namespace ArticleService.Data;

public class ArticleDbContext : DbContext
{
    public DbSet<Article> Articles => Set<Article>();


    public ArticleDbContext(DbContextOptions<ArticleDbContext> options) : base(options) { }


    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Article>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.Property(e => e.ShardKey).HasMaxLength(16);
            entity.HasIndex(e => e.ShardKey);
            entity.HasIndex(e => e.PublishedAt).IsDescending();
        });
    }
}