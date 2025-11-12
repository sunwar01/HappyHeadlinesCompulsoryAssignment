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

    private static readonly Counter ArticleCacheHits = Metrics.CreateCounter(
        "article_cache_hits_total", "Article cache hits",
        new CounterConfiguration { LabelNames = new[] { "region", "endpoint" } });

    private static readonly Counter ArticleCacheMisses = Metrics.CreateCounter(
        "article_cache_misses_total", "Article cache misses",
        new CounterConfiguration { LabelNames = new[] { "region", "endpoint" } });

    private static readonly Histogram RequestDuration = Metrics.CreateHistogram(
        "article_request_duration_seconds",
        "Duration of HTTP requests processed by ArticlesController",
        new HistogramConfiguration
        {
            LabelNames = new[] { "region", "method", "endpoint", "status" },
            Buckets = Histogram.ExponentialBuckets(0.005, 2, 12)
        });

    private static readonly Counter HttpErrors = Metrics.CreateCounter(
        "article_http_errors_total", "HTTP 5xx responses",
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
        region = (region ?? "global").ToLowerInvariant();
        var cs = Shards.PickConnection(region, _shards);
        var dbOpts = new DbContextOptionsBuilder<ArticleDbContext>().UseNpgsql(cs).Options;
        return new ArticleDbContext(dbOpts);
    }

    private async Task<ActionResult<T>> Measure<T>(string endpoint, Func<CancellationToken, Task<ActionResult<T>>> work, CancellationToken ct = default)
    {
        var routeRegion = ((string)RouteData.Values["region"]!)?.ToLowerInvariant() ?? "global";
        var method = HttpContext.Request.Method;
        var sw = System.Diagnostics.Stopwatch.StartNew();

        try
        {
            var result = await work(ct);
            var status = result switch
            {
                ObjectResult obj => obj.StatusCode?.ToString() ?? "200",
                StatusCodeResult sc => sc.StatusCode.ToString(),
                _ => "200"
            };

            RequestDuration
                .WithLabels(routeRegion, method, endpoint, status)
                .Observe(sw.Elapsed.TotalSeconds);

            if (status.StartsWith('5'))
                HttpErrors.WithLabels(routeRegion, endpoint).Inc();

            return result;
        }
        catch (Exception ex) when (!(ex is OperationCanceledException))
        {
            RequestDuration
                .WithLabels(routeRegion, method, endpoint, "500")
                .Observe(sw.Elapsed.TotalSeconds);

            HttpErrors.WithLabels(routeRegion, endpoint).Inc();
            throw;
        }
    }

    private async Task<IActionResult> Measure(string endpoint, Func<CancellationToken, Task<IActionResult>> work, CancellationToken ct = default)
    {
        var routeRegion = ((string)RouteData.Values["region"]!)?.ToLowerInvariant() ?? "global";
        var method = HttpContext.Request.Method;
        var sw = System.Diagnostics.Stopwatch.StartNew();

        try
        {
            var result = await work(ct);
            var status = result switch
            {
                ObjectResult obj => obj.StatusCode?.ToString() ?? "200",
                StatusCodeResult sc => sc.StatusCode.ToString(),
                _ => "200"
            };

            RequestDuration
                .WithLabels(routeRegion, method, endpoint, status)
                .Observe(sw.Elapsed.TotalSeconds);

            if (status.StartsWith('5'))
                HttpErrors.WithLabels(routeRegion, endpoint).Inc();

            return result;
        }
        catch (Exception ex) when (!(ex is OperationCanceledException))
        {
            RequestDuration
                .WithLabels(routeRegion, method, endpoint, "500")
                .Observe(sw.Elapsed.TotalSeconds);

            HttpErrors.WithLabels(routeRegion, endpoint).Inc();
            throw;
        }
    }

    [HttpGet]
    public Task<ActionResult<IEnumerable<Article>>> List(
        [FromRoute] string region, [FromQuery] int skip = 0, [FromQuery] int take = 20, CancellationToken ct = default)
    {
        region = (region ?? "global").ToLowerInvariant();

        return Measure<IEnumerable<Article>>("list", async token =>
        {
            take = Math.Clamp(take, 1, 100);

            try
            {
                var zkey = RecentKey(region);
                var ids = await _cache.SortedSetRangeByRankAsync(
                    zkey, skip, skip + take - 1, Order.Descending);

                if (ids.Length > 0)
                {
                    var guidIds = new List<Guid>(ids.Length);
                    foreach (var rv in ids)
                    {
                        if (!rv.IsNull && Guid.TryParse(rv.ToString(), out var gid))
                            guidIds.Add(gid);
                    }

                    if (guidIds.Count == 0)
                    {
                        ArticleCacheMisses.WithLabels(region, "list_index").Inc();
                        throw new InvalidOperationException("No valid GUIDs in recent index");
                    }

                    var keys = guidIds.Select(g => (RedisKey)AKey(region, g)).ToArray();
                    var blobs = await _cache.StringGetAsync(keys);

                    var hitCount = blobs.Count(b => b.HasValue);
                    var missCount = blobs.Length - hitCount;

                    for (int i = 0; i < hitCount; i++)
                        ArticleCacheHits.WithLabels(region, "list").Inc();

                    for (int i = 0; i < missCount; i++)
                        ArticleCacheMisses.WithLabels(region, "list").Inc();

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

                    if (dbMissIds.Count > 0)
                    {
                        await using var db = Ctx(region);
                        var fetched = await db.Articles
                            .Where(a => dbMissIds.Contains(a.Id))
                            .AsNoTracking()
                            .ToListAsync(token);

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

                        result = result
                            .OrderByDescending(a => a.PublishedAt)
                            .Take(take)
                            .ToList();
                    }

                    return Ok(result);
                }
                else
                {
                    ArticleCacheMisses.WithLabels(region, "list_index").Inc();
                }
            }
            catch (Exception ex)
            {
                _log.Warning(ex, "Article list cache read failed; falling back to DB");
            }

            await using (var db = Ctx(region))
            {
                var items = await db.Articles.AsNoTracking()
                    .OrderByDescending(a => a.PublishedAt)
                    .Skip(skip).Take(take)
                    .ToListAsync(token);

                for (int i = 0; i < items.Count; i++)
                    ArticleCacheMisses.WithLabels(region, "list").Inc();

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
        }, ct);
    }

    [HttpGet("{id:guid}")]
    public Task<ActionResult<Article>> Get([FromRoute] string region, Guid id, CancellationToken ct = default)
    {
        region = (region ?? "global").ToLowerInvariant();

        return Measure<Article>("get", async token =>
        {
            try
            {
                var blob = await _cache.StringGetAsync(AKey(region, id));
                if (blob.HasValue)
                {
                    ArticleCacheHits.WithLabels(region, "get").Inc();
                    var a = JsonSerializer.Deserialize<Article>(blob!, JsonOpts);
                    if (a != null) return Ok(a);
                }
            }
            catch (Exception ex)
            {
                _log.Debug(ex, "Article cache get failed");
            }

            await using var db = Ctx(region);
            var item = await db.Articles.AsNoTracking().FirstOrDefaultAsync(a => a.Id == id, token);
            if (item is null) return NotFound();

            ArticleCacheMisses.WithLabels(region, "get").Inc();

            try
            {
                await _cache.StringSetAsync(AKey(region, id),
                    JsonSerializer.Serialize(item, JsonOpts), ArticleTtl);

                await _cache.SortedSetAddAsync(
                    RecentKey(region), id.ToString(), item.PublishedAt.ToUnixTimeSeconds());
            }
            catch { }

            return Ok(item);
        }, ct);
    }

    [HttpPost]
    public Task<IActionResult> Create([FromRoute] string region, [FromBody] ArticleCreateDto dto, CancellationToken ct = default)
    {
        region = (region ?? "global").ToLowerInvariant();

        return Measure("create", async token =>
        {
            await using var db = Ctx(region);

            var entity = new Article
            {
                Id = Guid.NewGuid(),
                Title = dto.Title,
                Content = dto.Content,
                PublishedAt = dto.PublishedAt ?? DateTimeOffset.UtcNow,
                ShardKey = region,
            };

            db.Articles.Add(entity);
            await db.SaveChangesAsync(token);

            try
            {
                await _cache.StringSetAsync(AKey(region, entity.Id),
                    JsonSerializer.Serialize(entity, JsonOpts), ArticleTtl);

                await _cache.SortedSetAddAsync(
                    RecentKey(region), entity.Id.ToString(), entity.PublishedAt.ToUnixTimeSeconds());
            }
            catch { }

            return Created($"/api/{region}/articles/{entity.Id}", entity);
        }, ct);
    }

    [HttpPut("{id:guid}")]
    public Task<IActionResult> Update([FromRoute] string region, Guid id, [FromBody] ArticleCreateDto dto, CancellationToken ct = default)
    {
        region = (region ?? "global").ToLowerInvariant();

        return Measure("update", async token =>
        {
            await using var db = Ctx(region);
            var existing = await db.Articles.FirstOrDefaultAsync(a => a.Id == id, token);
            if (existing is null) return NotFound();

            existing.Title = dto.Title;
            existing.Content = dto.Content;
            existing.PublishedAt = dto.PublishedAt ?? existing.PublishedAt;

            await db.SaveChangesAsync(token);

            try
            {
                await _cache.StringSetAsync(AKey(region, id),
                    JsonSerializer.Serialize(existing, JsonOpts), ArticleTtl);

                await _cache.SortedSetAddAsync(
                    RecentKey(region), id.ToString(), existing.PublishedAt.ToUnixTimeSeconds());
            }
            catch { }

            return NoContent();
        }, ct);
    }

    [HttpDelete("{id:guid}")]
    public Task<IActionResult> Delete([FromRoute] string region, Guid id, CancellationToken ct = default)
    {
        region = (region ?? "global").ToLowerInvariant();

        return Measure("delete", async token =>
        {
            await using var db = Ctx(region);
            var entity = await db.Articles.FirstOrDefaultAsync(a => a.Id == id, token);
            if (entity is null) return NotFound();

            db.Articles.Remove(entity);
            await db.SaveChangesAsync(token);

            try
            {
                await _cache.KeyDeleteAsync(AKey(region, id));
                await _cache.SortedSetRemoveAsync(RecentKey(region), id.ToString());
            }
            catch { }

            return NoContent();
        }, ct);
    }
}
