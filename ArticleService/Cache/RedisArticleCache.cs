using System.Text.Json;
using ArticleService.Models;
using Prometheus;
using StackExchange.Redis;

namespace ArticleService.Cache;

public sealed class RedisArticleCache : IArticleCache
{
    private readonly IDatabase _db;
    private readonly TimeSpan _defaultTtl;

   
    private static readonly JsonSerializerOptions JsonOpts = new(JsonSerializerDefaults.Web);


    public RedisArticleCache(IConnectionMultiplexer mux, TimeSpan defaultTtl)
    {
        _db = mux.GetDatabase();
        _defaultTtl = defaultTtl;
    }

    private static string Key(string region, Guid id) => $"article:{region}:{id}";

    public async Task<Article?> GetAsync(string region, Guid id)
    {
      
        var val = await _db.StringGetAsync(Key(region, id));
        if (val.HasValue)
        {
        
            return JsonSerializer.Deserialize<Article>(val!, JsonOpts)!;
        }
    
        return null;
    }

    public Task SetAsync(string region, Article article, TimeSpan? ttl = null)
    {
        var blob = JsonSerializer.Serialize(article, JsonOpts);
        return _db.StringSetAsync(Key(region, article.Id), blob, ttl ?? _defaultTtl);
    }

    public async Task<int> SetManyAsync(string region, IEnumerable<Article> articles, TimeSpan? ttl = null)
    {
        var arr = articles as Article[] ?? articles.ToArray();
        if (arr.Length == 0) return 0;

        var entries = arr.Select(a =>
            new KeyValuePair<RedisKey, RedisValue>(Key(region, a.Id),
                JsonSerializer.Serialize(a, JsonOpts))
        ).ToArray();

  
        await _db.StringSetAsync(entries, when: When.Always);

        // Apply TTLs 
        var t = ttl ?? _defaultTtl;
        var batch = _db.CreateBatch();
        foreach (var a in arr)
            _ = batch.KeyExpireAsync(Key(region, a.Id), t);
        batch.Execute();

        return arr.Length;
    }
}