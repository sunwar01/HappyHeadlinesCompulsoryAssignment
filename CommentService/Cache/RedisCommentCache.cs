


using System.Text.Json;
using CommentService.Models;
using Prometheus;
using StackExchange.Redis;

namespace CommentService.Cache;

public sealed class RedisCommentCache : ICommentCache
{
    private readonly IDatabase _db;
    private readonly TimeSpan _ttl;
    private readonly int _capacity;
    private const string LruKey = "comments:lru";
    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

    private static readonly Counter Hits  = Metrics.CreateCounter("comment_cache_hit_total",  "Hits");
    private static readonly Counter Miss  = Metrics.CreateCounter("comment_cache_miss_total", "Misses");
    private static readonly Counter Evict = Metrics.CreateCounter("comment_cache_evict_total","Evicted keys");
    private static readonly Gauge   Size  = Metrics.CreateGauge("comment_cache_keys", "Keys in LRU");

    public RedisCommentCache(IConnectionMultiplexer mux, TimeSpan ttl, int capacity = 30)
    {
        _db = mux.GetDatabase();
        _ttl = ttl;
        _capacity = Math.Max(1, capacity);
    }

    private static string Key(Guid articleId) => $"comments:{articleId}";

    public async Task<(bool found, List<Comment>? comments)> TryGetAsync(Guid articleId)
    {
        var val = await _db.StringGetAsync(Key(articleId));
        if (val.HasValue)
        {
            Hits.Inc();
            await TouchAsync(articleId);
            var list = JsonSerializer.Deserialize<List<Comment>>(val!, JsonOptions) ?? new();
            return (true, list);
        }
        Miss.Inc();
        return (false, null);
    }

    public async Task PutAsync(Guid articleId, List<Comment> comments)
    {
        var json = JsonSerializer.Serialize(comments, JsonOptions);
        await _db.StringSetAsync(Key(articleId), json, _ttl);
        await TouchAsync(articleId);
        await EnforceCapacityAsync();
    }

    public async Task InvalidateAsync(Guid articleId)
    {
        await _db.KeyDeleteAsync(Key(articleId));
        await _db.SortedSetRemoveAsync(LruKey, articleId.ToString());
        Size.Set(await _db.SortedSetLengthAsync(LruKey));
    }

    private async Task TouchAsync(Guid articleId)
    {
        var nowTicks = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        await _db.SortedSetAddAsync(LruKey, articleId.ToString(), nowTicks);
        Size.Set(await _db.SortedSetLengthAsync(LruKey));
    }

    private async Task EnforceCapacityAsync()
    {
        var count = await _db.SortedSetLengthAsync(LruKey);
        if (count <= _capacity) return;

        var removeCount = (int)(count - _capacity);

        // Oldest first
        var victims = await _db.SortedSetRangeByRankAsync(
            LruKey, 0, removeCount - 1, Order.Ascending);

        foreach (var v in victims)
        {
            var idStr = v.ToString();
            if (Guid.TryParse(idStr, out var id))
            {
                await _db.KeyDeleteAsync(Key(id));
                Evict.Inc();
            }
        }

        // NOTE: no Order argument here — flags only (or omit for default)
        await _db.SortedSetRemoveRangeByRankAsync(LruKey, 0, victims.Length - 1);

        Size.Set(await _db.SortedSetLengthAsync(LruKey));
    }

}