namespace ArticleService.Models;

public class ArticleCreateDto
{
    public string Title { get; set; } = default!;
    public string Content { get; set; } = default!;
    public DateTimeOffset? PublishedAt { get; set; }
}