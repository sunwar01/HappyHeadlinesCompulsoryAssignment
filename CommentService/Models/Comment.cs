using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace CommentService.Models;

[Table("Comments")]
public class Comment
{
    [Key] public Guid Id { get; set; }
    [Required] public Guid ArticleId { get; set; }

    [Required, MaxLength(120)]
    public string Author { get; set; } = default!;

    [Required, MaxLength(4000)]
    public string Text { get; set; } = default!;

    [Required] public DateTimeOffset CreatedAt { get; set; } = DateTimeOffset.UtcNow;

    [Required, MaxLength(16)]
    public string Continent { get; set; } = "global";
}