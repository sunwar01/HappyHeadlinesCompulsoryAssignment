namespace PublisherService.Models;

public record ArticlePublished(Guid Id, string Title, string Content, string Region, DateTimeOffset PublishedAt);