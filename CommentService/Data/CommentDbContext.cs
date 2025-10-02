using CommentService.Models;
using Microsoft.EntityFrameworkCore;

namespace CommentService.Data;

public sealed class CommentDbContext : DbContext
{
    public CommentDbContext(DbContextOptions<CommentDbContext> options) : base(options) { }
    public DbSet<Comment> Comments => Set<Comment>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        var e = modelBuilder.Entity<Comment>();
        e.ToTable("Comments"); 
        e.HasKey(x => x.Id);
        e.Property(x => x.Author).HasMaxLength(120).IsRequired();
        e.Property(x => x.Text).HasMaxLength(4000).IsRequired();
        e.Property(x => x.Continent).HasMaxLength(16).IsRequired();
        e.HasIndex(x => x.ArticleId).HasDatabaseName("IX_Comments_ArticleId");
    }
}