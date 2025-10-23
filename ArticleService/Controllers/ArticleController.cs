using System.Text.Json;
using StackExchange.Redis;
using ArticleService.Data;
using ArticleService.Models;
using CommentService.Data;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Prometheus;
using Shared;
using ILogger = Serilog.ILogger;

namespace ArticleService.Controllers;

[ApiController]
[Route("api/{region}/[controller]")]
public class ArticlesController : ControllerBase
{
    private readonly ShardMap _shards;
    private readonly IDatabase _cache;
    private static readonly ILogger _log = MonitorService.Log.ForContext<ArticlesController>();

    private static readonly TimeSpan ArticleTtl = TimeSpan.FromDays(15);
    private static readonly JsonSerializerOptions JsonOpts = new(JsonSerializerDefaults.Web);

    // ---- Prometheus counters ----
    private static readonly Counter ArticleCacheHits = Metrics.CreateCounter(
        "article_cache_hits_total", "Article cache hits",
        new CounterConfiguration { LabelNames = new[] { "region", "endpoint" } });

    private static readonly Counter ArticleCacheMisses = Metrics.CreateCounter(
        "article_cache_misses_total",               
        "Article cache misses",
        new CounterConfiguration { LabelNames = new[] { "region", "endpoint" } });


    public ArticlesController(IConfiguration cfg, IConnectionMultiplexer redis)
    {
        _shards = new ShardMap
        {
            ConnectionStrings = cfg.GetSection("Shards:ConnectionStrings")
                                   .Get<Dictionary<string, string>>() ?? new()
        };
        _cache = redis.GetDatabase();
    }

    private static string AKey(string region, Guid id) => $"article:{region}:{id}";
    private static string RecentKey(string region) => $"articles:recent:{region}";

    private ArticleDbContext Ctx(string region)
    {
        var cs = Shards.PickConnection(region, _shards);
        var dbOpts = new DbContextOptionsBuilder<ArticleDbContext>().UseNpgsql(cs).Options;
        return new ArticleDbContext(dbOpts);
    }

    [HttpGet]
    public async Task<ActionResult<IEnumerable<Article>>> List(
        [FromRoute] string region, [FromQuery] int skip = 0, [FromQuery] int take = 20, CancellationToken ct = default)
    {
        using var act = MonitorService.ActivitySource.StartActivity("GET /articles");
        take = Math.Clamp(take, 1, 100);

        // 1) Try the cache index
        try
        {
            var zkey = RecentKey(region);
            var ids = await _cache.SortedSetRangeByRankAsync(
                zkey, skip, skip + take - 1, Order.Descending);

            if (ids.Length > 0)
            {
                // parse RedisValue -> Guid safely and keep a parallel list for index alignment
                var guidIds = new List<Guid>(ids.Length);
                foreach (var rv in ids)
                {
                    if (!rv.IsNull && Guid.TryParse(rv.ToString(), out var gid))
                        guidIds.Add(gid);
                }

                if (guidIds.Count == 0)
                {
                    // index existed but values were unusable -> treat as index miss
                    ArticleCacheMisses.Labels(region, "list_index").Inc();
                    throw new InvalidOperationException("No valid GUIDs in recent index");
                }

                // Fetch article JSONs in the SAME order
                var keys = guidIds.Select(g => (RedisKey)AKey(region, g)).ToArray();
                var blobs = await _cache.StringGetAsync(keys);

                // ---- hit/miss accounting (RIGHT HERE) ----
                var hitCount = blobs.Count(b => b.HasValue);
                var missCount = blobs.Length - hitCount;
                if (hitCount > 0)  ArticleCacheHits.Labels(region, "list").Inc(hitCount);
                if (missCount > 0) ArticleCacheMisses.Labels(region, "list").Inc(missCount);

                var result    = new List<Article>(blobs.Length);
                var dbMissIds = new List<Guid>();

                for (int i = 0; i < blobs.Length; i++)
                {
                    if (blobs[i].HasValue)
                    {
                        var a = JsonSerializer.Deserialize<Article>(blobs[i]!, JsonOpts);
                        if (a != null) result.Add(a);
                    }
                    else
                    {
                        dbMissIds.Add(guidIds[i]);
                    }
                }

                // Fill cache for any misses from DB
                if (dbMissIds.Count > 0)
                {
                    await using var db = Ctx(region);
                    var fetched = await db.Articles
                        .Where(a => dbMissIds.Contains(a.Id))
                        .ToListAsync(ct);

                    foreach (var a in fetched)
                    {
                        result.Add(a);
                        _ = _cache.StringSetAsync(
                            AKey(region, a.Id),
                            JsonSerializer.Serialize(a, JsonOpts),
                            ArticleTtl);
                        _ = _cache.SortedSetAddAsync(
                            zkey, a.Id.ToString(), a.PublishedAt.ToUnixTimeSeconds());
                    }

                    // Keep DB-like order
                    result = result
                        .OrderByDescending(a => a.PublishedAt)
                        .Take(take)
                        .ToList();
                }

                return Ok(result);
            }
            else
            {
                // nothing in the index at all -> count as index miss
                ArticleCacheMisses.Labels(region, "list_index").Inc();
            }
        }
        catch (Exception ex)
        {
            _log.Warning(ex, "Article list cache read failed; falling back to DB");
        }

        // 2) Fallback: DB
        await using (var db = Ctx(region))
        {
            var items = await db.Articles.AsNoTracking()
                .OrderByDescending(a => a.PublishedAt)
                .Skip(skip).Take(take)
                .ToListAsync(ct);

            ArticleCacheMisses.Labels(region, "list").Inc(items.Count);

            // Best-effort warmup
            try
            {
                var zkey = RecentKey(region);
                var tasks = new List<Task>(items.Count * 2);
                foreach (var a in items)
                {
                    tasks.Add(_cache.StringSetAsync(
                        AKey(region, a.Id),
                        JsonSerializer.Serialize(a, JsonOpts),
                        ArticleTtl));
                    tasks.Add(_cache.SortedSetAddAsync(
                        zkey, a.Id.ToString(), a.PublishedAt.ToUnixTimeSeconds()));
                }
                await Task.WhenAll(tasks);
            }
            catch (Exception ex)
            {
                _log.Debug(ex, "Article list warmup failed");
            }

            return Ok(items);
        }
    }

    [HttpGet("{id:guid}")]
    public async Task<ActionResult<Article>> Get([FromRoute] string region, Guid id, CancellationToken ct = default)
    {
        using var act = MonitorService.ActivitySource.StartActivity("GET /articles/{id}");

        // Cache first
        try
        {
            var blob = await _cache.StringGetAsync(AKey(region, id));
            if (blob.HasValue)
            {
                ArticleCacheHits.Labels(region, "get").Inc();
                var a = JsonSerializer.Deserialize<Article>(blob!, JsonOpts);
                if (a != null) return Ok(a);
            }
        }
        catch (Exception ex)
        {
            _log.Debug(ex, "Article cache get failed");
        }

        // DB fallback
        await using var db = Ctx(region);
        var item = await db.Articles.FirstOrDefaultAsync(a => a.Id == id, ct);
        if (item is null) return NotFound();

        ArticleCacheMisses.Labels(region, "get").Inc();

        // Write-through (best effort)
        try
        {
            await _cache.StringSetAsync(AKey(region, id),
                JsonSerializer.Serialize(item, JsonOpts), ArticleTtl);

            await _cache.SortedSetAddAsync(
                RecentKey(region), id.ToString(), item.PublishedAt.ToUnixTimeSeconds());
        }
        catch { /* ignore */ }

        return Ok(item);
    }

    [HttpPost]
    public async Task<IActionResult> Create([FromRoute] string region, [FromBody] ArticleCreateDto dto, CancellationToken ct = default)
    {
        using var act = MonitorService.ActivitySource.StartActivity("POST /articles");
        await using var db = Ctx(region);

        var entity = new Article
        {
            Id = Guid.NewGuid(),
            Title = dto.Title,
            Content = dto.Content,
            PublishedAt = dto.PublishedAt ?? DateTimeOffset.UtcNow,
            Continent = region,
        };

        db.Articles.Add(entity);
        await db.SaveChangesAsync(ct);

        // Write-through cache
        try
        {
            await _cache.StringSetAsync(AKey(region, entity.Id),
                JsonSerializer.Serialize(entity, JsonOpts), ArticleTtl);

            await _cache.SortedSetAddAsync(
                RecentKey(region), entity.Id.ToString(), entity.PublishedAt.ToUnixTimeSeconds());
        }
        catch { /* ignore */ }

        return Created($"/api/{region}/articles/{entity.Id}", entity);
    }

    [HttpPut("{id:guid}")]
    public async Task<IActionResult> Update([FromRoute] string region, Guid id, [FromBody] ArticleCreateDto dto, CancellationToken ct = default)
    {
        using var act = MonitorService.ActivitySource.StartActivity("PUT /articles/{id}");
        await using var db = Ctx(region);
        var existing = await db.Articles.FirstOrDefaultAsync(a => a.Id == id, ct);
        if (existing is null) return NotFound();

        existing.Title = dto.Title;
        existing.Content = dto.Content;
        existing.PublishedAt = dto.PublishedAt ?? existing.PublishedAt;

        await db.SaveChangesAsync(ct);

        // Write-through cache
        try
        {
            await _cache.StringSetAsync(AKey(region, id),
                JsonSerializer.Serialize(existing, JsonOpts), ArticleTtl);

            await _cache.SortedSetAddAsync(
                RecentKey(region), id.ToString(), existing.PublishedAt.ToUnixTimeSeconds());
        }
        catch { /* ignore */ }

        return NoContent();
    }

    [HttpDelete("{id:guid}")]
    public async Task<IActionResult> Delete([FromRoute] string region, Guid id, CancellationToken ct = default)
    {
        using var act = MonitorService.ActivitySource.StartActivity("DELETE /articles/{id}");
        await using var db = Ctx(region);
        var entity = await db.Articles.FirstOrDefaultAsync(a => a.Id == id, ct);
        if (entity is null) return NotFound();

        db.Articles.Remove(entity);
        await db.SaveChangesAsync(ct);

        // Invalidate cache
        try
        {
            await _cache.KeyDeleteAsync(AKey(region, id));
            await _cache.SortedSetRemoveAsync(RecentKey(region), id.ToString());
        }
        catch { /* ignore */ }

        return NoContent();
    }
}
