using System.Collections.Concurrent;
using System.Text.Json;
using ArticleService.Data;
using CommentService.Data;
using CommentService.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using Prometheus;
using Shared;
using StackExchange.Redis;
using ILogger = Serilog.ILogger;

namespace CommentService.Controllers;

[ApiController]
[Route("api/{region}/articles/{articleId:guid}/comments")]
public class CommentsController : ControllerBase
{
    private readonly ShardMap _shards;
    private readonly IDatabase _cache;
    private static readonly ILogger Log = MonitorService.Log.ForContext<CommentsController>();

    private const int Capacity = 30; // per region LRU capacity
    private static readonly TimeSpan CommentsTtl = TimeSpan.FromHours(12);
    private static readonly JsonSerializerOptions JsonOpts = new(JsonSerializerDefaults.Web);

    // ---- Prometheus counters ----
    private static readonly Counter CommentCacheHits = Metrics.CreateCounter(
        "comment_cache_hits_total", "Comment cache hits",
        new CounterConfiguration { LabelNames = new[] { "region", "endpoint" } });

    private static readonly Counter CommentCacheMisses = Metrics.CreateCounter(
        "comment_cache_misses_total", "Comment cache misses",
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

    public CommentsController(IConfiguration cfg, IConnectionMultiplexer redis)
    {
        _shards = new ShardMap
        {
            ConnectionStrings = cfg.GetSection("Shards:ConnectionStrings")
                                   .Get<Dictionary<string, string>>() ?? new()
        };
        _cache = redis.GetDatabase();
    }

    private static string ListKey(string region, Guid articleId) => $"comments:{region}:{articleId}";
    private static string LruKey(string region) => $"comments:lru:{region}";

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
            {
                await _cache.KeyDeleteAsync(ListKey(region, aid));
            }
        }

        if (victims.Length > 0)
            await _cache.SortedSetRemoveRangeByRankAsync(lru, 0, victims.Length - 1);
    }

    // GET /api/{region}/articles/{articleId}/comments
    [HttpGet]
    public async Task<ActionResult<IEnumerable<Comment>>> List(
        [FromRoute] string region,
        [FromRoute] Guid articleId,
        [FromQuery] int skip = 0,
        [FromQuery] int take = 50,
        CancellationToken ct = default)
    {
        var cs = Shards.PickConnection(region, _shards);
        await EnsureShardSchemaAsync(cs, ct);

        // Cache first (whole article list)
        try
        {
            var blob = await _cache.StringGetAsync(ListKey(region, articleId));
            if (blob.HasValue)
            {
                CommentCacheHits.Labels(region, "list").Inc();

                var all = JsonSerializer.Deserialize<List<Comment>>(blob!, JsonOpts) ?? new();
                await TouchLruAsync(region, articleId);

                var paged = all
                    .OrderByDescending(c => c.CreatedAt)
                    .Skip(skip)
                    .Take(Math.Clamp(take, 1, 200))
                    .ToList();

                return Ok(paged);
            }
        }
        catch (Exception ex)
        {
            Log.Debug(ex, "Comment list cache get failed");
        }

        CommentCacheMisses.Labels(region, "list").Inc();

        // Miss -> DB, then cache whole list
        await using (var db = Ctx(region))
        {
            var all = await db.Comments.AsNoTracking()
                .Where(c => c.ArticleId == articleId)
                .OrderByDescending(c => c.CreatedAt)
                .ToListAsync(ct);

            // Best-effort populate + LRU
            try
            {
                await _cache.StringSetAsync(
                    ListKey(region, articleId),
                    JsonSerializer.Serialize(all, JsonOpts),
                    CommentsTtl);

                await TouchLruAsync(region, articleId);
            }
            catch { /* ignore */ }

            var paged = all
                .Skip(skip)
                .Take(Math.Clamp(take, 1, 200))
                .ToList();

            return Ok(paged);
        }
    }

    // GET /api/{region}/articles/{articleId}/comments/{id}
    [HttpGet("{id:guid}")]
    public async Task<ActionResult<Comment>> Get(
        [FromRoute] string region,
        [FromRoute] Guid articleId,
        [FromRoute] Guid id,
        CancellationToken ct = default)
    {
        var cs = Shards.PickConnection(region, _shards);
        await EnsureShardSchemaAsync(cs, ct);

        // Try cached list first
        try
        {
            var blob = await _cache.StringGetAsync(ListKey(region, articleId));
            if (blob.HasValue)
            {
                var all = JsonSerializer.Deserialize<List<Comment>>(blob!, JsonOpts) ?? new();
                var hit = all.FirstOrDefault(c => c.Id == id);
                if (hit != null)
                {
                    CommentCacheHits.Labels(region, "get").Inc();
                    await TouchLruAsync(region, articleId);
                    return Ok(hit);
                }

                // list cached but specific id not found -> miss for the id
                CommentCacheMisses.Labels(region, "get").Inc();
            }
        }
        catch { /* ignore */ }

        // DB fallback
        await using var db = Ctx(region);

        var item = await db.Comments
            .FirstOrDefaultAsync(c => c.ArticleId == articleId && c.Id == id, ct);

        if (item is null) return NotFound();

        try { await TouchLruAsync(region, articleId); } catch { /* ignore */ }

        return Ok(item);
    }

    // POST /api/{region}/articles/{articleId}/comments
    [HttpPost]
    public async Task<IActionResult> Create(
        [FromRoute] string region,
        [FromRoute] Guid articleId,
        [FromBody] CommentCreateDto dto,
        CancellationToken ct = default)
    {
        var cs = Shards.PickConnection(region, _shards);
        await EnsureShardSchemaAsync(cs, ct);

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
        await db.SaveChangesAsync(ct);

        // Invalidate that article's cached list (cache-miss approach)
        try
        {
            await _cache.KeyDeleteAsync(ListKey(region, articleId));
            await _cache.SortedSetRemoveAsync(LruKey(region), articleId.ToString());
        }
        catch { /* ignore */ }

        return CreatedAtAction(nameof(Get), new { region, articleId, id = entity.Id }, entity);
    }

    // PUT /api/{region}/articles/{articleId}/comments/{id}
    [HttpPut("{id:guid}")]
    public async Task<IActionResult> Update(
        [FromRoute] string region,
        [FromRoute] Guid articleId,
        [FromRoute] Guid id,
        [FromBody] CommentCreateDto dto,
        CancellationToken ct = default)
    {
        var cs = Shards.PickConnection(region, _shards);
        await EnsureShardSchemaAsync(cs, ct);

        await using var db = Ctx(region);

        var existing = await db.Comments
            .FirstOrDefaultAsync(c => c.ArticleId == articleId && c.Id == id, ct);

        if (existing is null) return NotFound();

        existing.Author = dto.Author;
        existing.Text = dto.Text;
        existing.CreatedAt = dto.CreatedAt ?? existing.CreatedAt;

        await db.SaveChangesAsync(ct);

        // Invalidate cached list so next GET repopulates
        try
        {
            await _cache.KeyDeleteAsync(ListKey(region, articleId));
            await _cache.SortedSetRemoveAsync(LruKey(region), articleId.ToString());
        }
        catch { /* ignore */ }

        return NoContent();
    }

    // DELETE /api/{region}/articles/{articleId}/comments/{id}
    [HttpDelete("{id:guid}")]
    public async Task<IActionResult> Delete(
        [FromRoute] string region,
        [FromRoute] Guid articleId,
        [FromRoute] Guid id,
        CancellationToken ct = default)
    {
        var cs = Shards.PickConnection(region, _shards);
        await EnsureShardSchemaAsync(cs, ct);

        await using var db = Ctx(region);

        var entity = await db.Comments
            .FirstOrDefaultAsync(c => c.ArticleId == articleId && c.Id == id, ct);

        if (entity is null) return NotFound();

        db.Comments.Remove(entity);
        await db.SaveChangesAsync(ct);

        // Invalidate cached list + LRU entry
        try
        {
            await _cache.KeyDeleteAsync(ListKey(region, articleId));
            await _cache.SortedSetRemoveAsync(LruKey(region), articleId.ToString());
        }
        catch { /* ignore */ }

        return NoContent();
    }
}
