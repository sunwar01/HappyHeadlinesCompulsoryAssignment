using System.ComponentModel.DataAnnotations;

namespace ArticleService.Models;

public class Article
{
    public Guid Id { get; set; } = Guid.NewGuid();


    [Required]
    [MaxLength(200)]
    public string Title { get; set; } = default!;


    [Required]
    public string Content { get; set; } = default!;

    
    [Required]
    [MaxLength(16)] 
    public string Continent { get; set; } = "global";


    public DateTimeOffset PublishedAt { get; set; } = DateTimeOffset.UtcNow;
}