using ArticleService.Models;

namespace ArticleService.Cache;

public interface IArticleCache
{
    Task<Article?> GetAsync(string region, Guid id);
    Task SetAsync(string region, Article article, TimeSpan? ttl = null);
    Task<int> SetManyAsync(string region, IEnumerable<Article> articles, TimeSpan? ttl = null);
}