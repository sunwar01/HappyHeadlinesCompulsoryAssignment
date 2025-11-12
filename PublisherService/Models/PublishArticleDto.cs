namespace PublisherService.Models;

public record PublishArticleDto(string Title, string Content, DateTimeOffset? PublishedAt);