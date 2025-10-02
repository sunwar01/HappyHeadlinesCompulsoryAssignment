using System.Text.Json;
using ArticleService.Models;
using Prometheus;
using StackExchange.Redis;

namespace ArticleService.Cache;

public sealed class RedisArticleCache : IArticleCache
{
    private readonly IDatabase _db;
    private readonly TimeSpan _defaultTtl;
    private static readonly Counter Hit = Metrics.CreateCounter("article_cache_hit_total", "Hits");
    private static readonly Counter Miss = Metrics.CreateCounter("article_cache_miss_total", "Misses");

    public RedisArticleCache(IConnectionMultiplexer mux, TimeSpan defaultTtl)
    {
        _db = mux.GetDatabase();
        _defaultTtl = defaultTtl;
    }

    private static string Key(Guid id) => $"article:{id}";

    public async Task<Article?> GetAsync(Guid id)
    {
        var val = await _db.StringGetAsync(Key(id));
        if (val.HasValue)
        {
            Hit.Inc();
            return JsonSerializer.Deserialize<Article>(val!)!;
        }
        Miss.Inc();
        return null;
    }

    public Task SetAsync(Article article, TimeSpan? ttl = null)
        => _db.StringSetAsync(Key(article.Id), JsonSerializer.Serialize(article), ttl ?? _defaultTtl);

    public async Task<int> SetManyAsync(IEnumerable<Article> articles, TimeSpan? ttl = null)
    {
        var list = articles.ToList();
        var t = ttl ?? _defaultTtl;
        var batch = _db.CreateBatch();
        foreach (var a in list)
            _ = batch.StringSetAsync(Key(a.Id), JsonSerializer.Serialize(a), t);
        batch.Execute();
        return list.Count;
    }
}