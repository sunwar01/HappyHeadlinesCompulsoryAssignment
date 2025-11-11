using System.Collections.Concurrent;
using System.Text.Json;
using ArticleService.Data;
using CommentService.Data;
using CommentService.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using Polly.CircuitBreaker;
using Prometheus;
using Shared;
using StackExchange.Redis;
using ILogger = Serilog.ILogger;
using Polly.Timeout; 

namespace CommentService.Controllers;

[ApiController]
[Route("api/{region}/articles/{articleId:guid}/[controller]")]
public class CommentsController : ControllerBase
{
    private readonly IHttpClientFactory _httpFactory;
    private readonly ShardMap _shards;
    private readonly IDatabase _cache;
    private static readonly ILogger Log = MonitorService.Log.ForContext<CommentsController>();

    private const int Capacity = 30;
    private static readonly TimeSpan CommentsTtl = TimeSpan.FromHours(12);
    private static readonly JsonSerializerOptions JsonOpts = new(JsonSerializerDefaults.Web);

    // ── PROMETHEUS METRICS ─────────────────────────────────────────────────────
    private static readonly Counter CommentCacheHits = Metrics.CreateCounter(
        "comment_cache_hits_total", "Comment cache hits",
        new CounterConfiguration { LabelNames = new[] { "region", "endpoint" } });

    private static readonly Counter CommentCacheMisses = Metrics.CreateCounter(
        "comment_cache_misses_total", "Comment cache misses",
        new CounterConfiguration { LabelNames = new[] { "region", "endpoint" } });

    private static readonly Histogram RequestDuration = Metrics.CreateHistogram(
        "comment_request_duration_seconds",
        "Duration of HTTP requests in CommentsController",
        new HistogramConfiguration
        {
            LabelNames = new[] { "region", "method", "endpoint", "status" },
            Buckets = Histogram.ExponentialBuckets(0.005, 2, 12)
        });

    private static readonly Counter HttpErrors = Metrics.CreateCounter(
        "comment_http_errors_total", "HTTP 5xx responses",
        new CounterConfiguration { LabelNames = new[] { "region", "endpoint" } });

    private static readonly ConcurrentDictionary<string, bool> SchemaReady = new();

    private const string Ddl = """
        CREATE TABLE IF NOT EXISTS public."Comments" (
            "Id" uuid PRIMARY KEY,
            "ArticleId" uuid NOT NULL,
            "Author" varchar(120) NOT NULL,
            "Text" varchar(4000) NOT NULL,
            "Continent" varchar(16) NOT NULL,
            "CreatedAt" timestamptz NOT NULL
        );
        CREATE INDEX IF NOT EXISTS "IX_Comments_ArticleId" ON public."Comments" ("ArticleId");
        """;

    public CommentsController(IConfiguration cfg, IConnectionMultiplexer redis, IHttpClientFactory httpFactory)
    {
        _shards = new ShardMap
        {
            ConnectionStrings = cfg.GetSection("Shards:ConnectionStrings")
                                   .Get<Dictionary<string, string>>() ?? new()
        };
        _cache = redis.GetDatabase();
        _httpFactory = httpFactory;
    }

    private static string ListKey(string region, Guid articleId) => $"comments:{region}:{articleId}";
    private static string LruKey(string region) => $"comments:lru:{region}";

    private CommentDbContext Ctx(string region)
    {
        var cs = Shards.PickConnection(region, _shards);
        var opts = new DbContextOptionsBuilder<CommentDbContext>().UseNpgsql(cs).Options;
        return new CommentDbContext(opts);
    }

    private async Task TouchLruAsync(string region, Guid articleId)
    {
        var nowScore = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        await _cache.SortedSetAddAsync(LruKey(region), articleId.ToString(), nowScore);
        await EnforceCapacityAsync(region);
    }

    private async Task EnforceCapacityAsync(string region)
    {
        var lru = LruKey(region);
        var count = await _cache.SortedSetLengthAsync(lru);
        if (count <= Capacity) return;

        var removeCount = (int)(count - Capacity);
        var victims = await _cache.SortedSetRangeByRankAsync(lru, 0, removeCount - 1, Order.Ascending);

        foreach (var v in victims)
        {
            if (Guid.TryParse(v!, out var aid))
                await _cache.KeyDeleteAsync(ListKey(region, aid));
        }

        if (victims.Length > 0)
            await _cache.SortedSetRemoveRangeByRankAsync(lru, 0, victims.Length - 1);
    }

    // ── Measure: latency + error tracking ─────────────────────────────────────
    private async Task<ActionResult<T>> Measure<T>(string endpoint,
        Func<CancellationToken, Task<ActionResult<T>>> work,
        CancellationToken ct = default)
    {
        var region = (string)RouteData.Values["region"]!;
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

            RequestDuration.WithLabels(region, method, endpoint, status)
                           .Observe(sw.Elapsed.TotalSeconds);
            if (status.StartsWith('5'))
                HttpErrors.WithLabels(region, endpoint).Inc();

            return result;
        }
        catch (Exception ex) when (!(ex is OperationCanceledException))
        {
            RequestDuration.WithLabels(region, method, endpoint, "500")
                           .Observe(sw.Elapsed.TotalSeconds);
            HttpErrors.WithLabels(region, endpoint).Inc();
            throw;
        }
    }

    private async Task<IActionResult> Measure(string endpoint,
        Func<CancellationToken, Task<IActionResult>> work,
        CancellationToken ct = default)
    {
        var region = (string)RouteData.Values["region"]!;
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

            RequestDuration.WithLabels(region, method, endpoint, status)
                           .Observe(sw.Elapsed.TotalSeconds);
            if (status.StartsWith('5'))
                HttpErrors.WithLabels(region, endpoint).Inc();

            return result;
        }
        catch (Exception ex) when (!(ex is OperationCanceledException))
        {
            RequestDuration.WithLabels(region, method, endpoint, "500")
                           .Observe(sw.Elapsed.TotalSeconds);
            HttpErrors.WithLabels(region, endpoint).Inc();
            throw;
        }
    }

    // ── GET /api/{region}/articles/{articleId}/comments ─────────────────────
    [HttpGet]
    public Task<ActionResult<IEnumerable<Comment>>> List(
        [FromRoute] string region,
        [FromRoute] Guid articleId,
        [FromQuery] int skip = 0,
        [FromQuery] int take = 50,
        CancellationToken ct = default)
    {
        return Measure<IEnumerable<Comment>>("list", async token =>
        {
            var cs = Shards.PickConnection(region, _shards);
            await EnsureShardSchemaAsync(cs, token);

            // Try cache first
            List<Comment>? comments = null;
            try
            {
                var blob = await _cache.StringGetAsync(ListKey(region, articleId));
                if (blob.HasValue)
                {
                    CommentCacheHits.WithLabels(region, "list").Inc();
                    comments = JsonSerializer.Deserialize<List<Comment>>(blob!, JsonOpts) ?? new();
                    await TouchLruAsync(region, articleId);
                }
            }
            catch (Exception ex)
            {
                Log.Debug(ex, "Comment list cache get failed");
            }

            // Use cached data if available
            if (comments != null)
            {
                var result = comments
                    .OrderByDescending(c => c.CreatedAt)
                    .Skip(skip)
                    .Take(Math.Clamp(take, 1, 200))
                    .ToList();

                return Ok(result);
            }

            // Cache miss → DB
            CommentCacheMisses.WithLabels(region, "list").Inc();

            await using var db = Ctx(region);
            var dbComments = await db.Comments.AsNoTracking()
                .Where(c => c.ArticleId == articleId)
                .OrderByDescending(c => c.CreatedAt)
                .ToListAsync(token);

            // Warm cache
            try
            {
                await _cache.StringSetAsync(
                    ListKey(region, articleId),
                    JsonSerializer.Serialize(dbComments, JsonOpts),
                    CommentsTtl);
                await TouchLruAsync(region, articleId);
            }
            catch { }

            // Return paged result
            var finalResult = dbComments
                .Skip(skip)
                .Take(Math.Clamp(take, 1, 200))
                .ToList();

            return Ok(finalResult);
        }, ct);
    }

    // ── GET /api/{region}/articles/{articleId}/comments/{id} ─────────────────
    [HttpGet("{id:guid}")]
    public Task<ActionResult<Comment>> Get(
        [FromRoute] string region,
        [FromRoute] Guid articleId,
        [FromRoute] Guid id,
        CancellationToken ct = default)
    {
        return Measure<Comment>("get", async token =>
        {
            var cs = Shards.PickConnection(region, _shards);
            await EnsureShardSchemaAsync(cs, token);

            try
            {
                var blob = await _cache.StringGetAsync(ListKey(region, articleId));
                if (blob.HasValue)
                {
                    var cached = JsonSerializer.Deserialize<List<Comment>>(blob!, JsonOpts) ?? new();
                    var hit = cached.FirstOrDefault(c => c.Id == id);
                    if (hit != null)
                    {
                        CommentCacheHits.WithLabels(region, "get").Inc();
                        await TouchLruAsync(region, articleId);
                        return Ok(hit);
                    }
                    CommentCacheMisses.WithLabels(region, "get").Inc();
                }
            }
            catch { }

            await using var db = Ctx(region);
            var item = await db.Comments
                .FirstOrDefaultAsync(c => c.ArticleId == articleId && c.Id == id, token);

            if (item is null) return NotFound();

            await TouchLruAsync(region, articleId);
            return Ok(item);
        }, ct);
    }

    // ── POST /api/{region}/articles/{articleId}/comments ─────────────────────
 [HttpPost]
public async Task<IActionResult> Create(
    [FromRoute] string region,
    [FromRoute] Guid articleId,
    [FromBody] CommentCreateDto dto,
    CancellationToken ct = default)
{
    try
    {
        return await Measure("create", async token =>
        {
            var cs = Shards.PickConnection(region, _shards);
            await EnsureShardSchemaAsync(cs, token);

            var client = _httpFactory.CreateClient("ProfanityClient");
            var profanityReq = new { text = dto.Text };
            ProfanityResult? result = null;

            try
            {
                var resp = await client.PostAsJsonAsync("/api/profanity/check", profanityReq, token);
                if (resp.IsSuccessStatusCode)
                {
                    result = await resp.Content.ReadFromJsonAsync<ProfanityResult>(cancellationToken: token);
                }
                else
                {
                    Log.Warning("Profanity check failed (Status: {StatusCode}). Allowing comment.", resp.StatusCode);
                }
            }
            catch (BrokenCircuitException ex)
            {
                Log.Warning("CIRCUIT BREAKER OPEN: {Message}. Allowing comment.", ex.Message);
            }
            catch (TimeoutRejectedException ex)            
            {
                Log.Warning(ex, "Polly timeout hit. Allowing comment.");
            }
            catch (HttpRequestException ex)
            {
                Log.Warning(ex, "Profanity service unreachable. Allowing comment.");
            }
            catch (TaskCanceledException ex) when (!ex.CancellationToken.IsCancellationRequested)
            {
                Log.Warning(ex, "HttpClient timeout. Allowing comment.");
            }

            if (result?.IsProfane == true)
            {
                return BadRequest(new { error = "Profanity detected", matches = result.Matches });
            }

            await using var db = Ctx(region);
            var entity = new Comment
            {
                Id = Guid.NewGuid(),
                ArticleId = articleId,
                Author = dto.Author,
                Text = dto.Text,
                CreatedAt = dto.CreatedAt ?? DateTimeOffset.UtcNow,
                Continent = region
            };

            db.Comments.Add(entity);
            await db.SaveChangesAsync(token);

            try
            {
                await _cache.KeyDeleteAsync(ListKey(region, articleId));
                await _cache.SortedSetRemoveAsync(LruKey(region), articleId.ToString());
            }
            catch { }

            return CreatedAtAction(nameof(Get), new { region, articleId, id = entity.Id }, entity);
        }, ct);
    }
    catch (Exception ex)
    {
        Log.Error(ex, "Unhandled error in Create action");
        return StatusCode(500, new { error = "Internal error" });
    }
}

    // ── PUT /api/{region}/articles/{articleId}/comments/{id} ─────────────────
    [HttpPut("{id:guid}")]
    public Task<IActionResult> Update(
        [FromRoute] string region,
        [FromRoute] Guid articleId,
        [FromRoute] Guid id,
        [FromBody] CommentCreateDto dto,
        CancellationToken ct = default)
    {
        return Measure("update", async token =>
        {
            var cs = Shards.PickConnection(region, _shards);
            await EnsureShardSchemaAsync(cs, token);

            await using var db = Ctx(region);
            var existing = await db.Comments
                .FirstOrDefaultAsync(c => c.ArticleId == articleId && c.Id == id, token);

            if (existing is null) return NotFound();

            existing.Author = dto.Author;
            existing.Text = dto.Text;
            existing.CreatedAt = dto.CreatedAt ?? existing.CreatedAt;

            await db.SaveChangesAsync(token);

            try
            {
                await _cache.KeyDeleteAsync(ListKey(region, articleId));
                await _cache.SortedSetRemoveAsync(LruKey(region), articleId.ToString());
            }
            catch { }

            return NoContent();
        }, ct);
    }

    // ── DELETE /api/{region}/articles/{articleId}/comments/{id} ─────────────
    [HttpDelete("{id:guid}")]
    public Task<IActionResult> Delete(
        [FromRoute] string region,
        [FromRoute] Guid articleId,
        [FromRoute] Guid id,
        CancellationToken ct = default)
    {
        return Measure("delete", async token =>
        {
            var cs = Shards.PickConnection(region, _shards);
            await EnsureShardSchemaAsync(cs, token);

            await using var db = Ctx(region);
            var entity = await db.Comments
                .FirstOrDefaultAsync(c => c.ArticleId == articleId && c.Id == id, token);

            if (entity is null) return NotFound();

            db.Comments.Remove(entity);
            await db.SaveChangesAsync(token);

            try
            {
                await _cache.KeyDeleteAsync(ListKey(region, articleId));
                await _cache.SortedSetRemoveAsync(LruKey(region), articleId.ToString());
            }
            catch { }

            return NoContent();
        }, ct);
    }

    // ── Ensure schema exists once per shard ─────────────────────────────────
    private static async Task EnsureShardSchemaAsync(string cs, CancellationToken ct)
    {
        if (SchemaReady.ContainsKey(cs)) return;

        await using var conn = new NpgsqlConnection(cs);
        await conn.OpenAsync(ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = Ddl;
        await cmd.ExecuteNonQueryAsync(ct);

        SchemaReady.TryAdd(cs, true);
    }
}