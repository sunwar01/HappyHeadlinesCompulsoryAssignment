namespace CommentService.Models;

public class CommentCreateDto
{
    public string Author { get; set; } = default!;
    public string Text { get; set; } = default!;
    public DateTimeOffset? CreatedAt { get; set; }
}
