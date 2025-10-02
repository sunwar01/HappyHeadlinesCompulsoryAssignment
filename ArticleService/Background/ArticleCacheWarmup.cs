using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using ArticleService.Data;
using ArticleService.Models;
using CommentService.Data;
using Shared;

namespace ArticleService.Background;

public sealed class ArticleCacheWarmup : BackgroundService
{
    private readonly ILogger<ArticleCacheWarmup> _log;
    private readonly IDatabase _cache;
    private readonly ShardMap _shards;
    private static readonly TimeSpan Period = TimeSpan.FromMinutes(10);     // run every 10 min
    private static readonly TimeSpan Lookback = TimeSpan.FromDays(14);      // last 14 days
    private static readonly TimeSpan ArticleTtl = TimeSpan.FromDays(15);    // keep a bit longer than lookback
    private static readonly JsonSerializerOptions JsonOpts = new(JsonSerializerDefaults.Web);

    public ArticleCacheWarmup(ILogger<ArticleCacheWarmup> log, IConnectionMultiplexer redis, IConfiguration cfg)
    {
        _log = log;
        _cache = redis.GetDatabase();
        _shards = new ShardMap {
            ConnectionStrings = cfg.GetSection("Shards:ConnectionStrings")
                                   .Get<Dictionary<string,string>>() ?? new()
        };
    }

    private static string AKey(string region, Guid id) => $"article:{region}:{id}";
    private static string RecentKey(string region) => $"articles:recent:{region}";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // run once on startup, then periodically
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await WarmAllShards(stoppingToken);
            }
            catch (Exception ex)
            {
                _log.LogWarning(ex, "ArticleCache warmup failed; will retry next cycle");
            }

            try { await Task.Delay(Period, stoppingToken); }
            catch (TaskCanceledException) { break; }
        }
    }

    private async Task WarmAllShards(CancellationToken ct)
    {
        var since = DateTimeOffset.UtcNow - Lookback;
        foreach (var (region, cs) in _shards.ConnectionStrings)
        {
            await using var db = new ArticleDbContext(
                new DbContextOptionsBuilder<ArticleDbContext>().UseNpgsql(cs).Options);

            // Pull last 14 days (cap to avoid runaway)
            var recent = await db.Articles.AsNoTracking()
                .Where(a => a.PublishedAt >= since)
                .OrderByDescending(a => a.PublishedAt)
                .Take(2000)
                .ToListAsync(ct);

            if (recent.Count == 0) continue;

            var zkey = RecentKey(region);
            var ops = new List<Task>(recent.Count * 2);

            foreach (var a in recent)
            {
                ops.Add(_cache.StringSetAsync(AKey(region, a.Id),
                    JsonSerializer.Serialize(a, JsonOpts), ArticleTtl));

                ops.Add(_cache.SortedSetAddAsync(
                    zkey, a.Id.ToString(), a.PublishedAt.ToUnixTimeSeconds()));
            }

            await Task.WhenAll(ops);

            // Optional: trim the `recent` index to a sane size (e.g., keep newest 5000)
            var length = await _cache.SortedSetLengthAsync(zkey);
            const long keep = 5000;
            if (length > keep)
            {
                await _cache.SortedSetRemoveRangeByRankAsync(zkey, 0, (length - keep) - 1);
            }

            _log.LogInformation("ArticleCache warmup: region {Region} cached {Count} articles", region, recent.Count);
        }
    }
}