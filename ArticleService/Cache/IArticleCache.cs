using ArticleService.Models;

namespace ArticleService.Cache;

public interface IArticleCache
{
    Task<Article?> GetAsync(Guid id);
    Task SetAsync(Article article, TimeSpan? ttl = null);
    Task<int> SetManyAsync(IEnumerable<Article> articles, TimeSpan? ttl = null);
}