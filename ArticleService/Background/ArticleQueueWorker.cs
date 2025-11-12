// ArticleService/Background/ArticleQueueWorker.cs
using Shared;
using System.Diagnostics;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using OpenTelemetry;
using OpenTelemetry.Context.Propagation;
using Serilog;
using StackExchange.Redis;
using ArticleService.Data;
using ArticleService.Models;

namespace ArticleService.Background;

public class ArticleQueueWorker : BackgroundService
{
    private static readonly TextMapPropagator Propagator = Propagators.DefaultTextMapPropagator;
    private static readonly ActivitySource ActivitySource = new("ArticleService.Worker");

    private readonly IConnectionMultiplexer _redis;
    private readonly IConfiguration _cfg;
    private readonly Serilog.ILogger _log = MonitorService.Log.ForContext<ArticleQueueWorker>();

    private static readonly string[] Regions =
    { "global","europe","asia","africa","northamerica","southamerica","australia","antarctica" };

    public ArticleQueueWorker(IConnectionMultiplexer redis, IConfiguration cfg)
    {
        _redis = redis;
        _cfg = cfg;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Force W3C propagation here as well
        Sdk.SetDefaultTextMapPropagator(new TraceContextPropagator());

        var db = _redis.GetDatabase();

        // Ensure groups exist
        foreach (var r in Regions)
        {
            var stream = (RedisKey)$"article_queue:{r}";
            try { await db.StreamCreateConsumerGroupAsync(stream, "articlesvc", "0-0"); }
            catch (RedisServerException ex) when (ex.Message.Contains("BUSYGROUP")) { }
        }

        var jsonOpts = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };

        while (!stoppingToken.IsCancellationRequested)
        {
            foreach (var region in Regions)
            {
                var stream = (RedisKey)$"article_queue:{region}";
                StreamEntry[] entries;
                try
                {
                    entries = await db.StreamReadGroupAsync(stream, "articlesvc", "worker-1", ">", 20);
                }
                catch (Exception ex)
                {
                    _log.Warning(ex, "StreamReadGroup failed for {Stream}", (string)stream);
                    continue;
                }

                if (entries.Length == 0) continue;

                foreach (var e in entries)
                {
                    var bodyRv        = e.Values.FirstOrDefault(v => v.Name == "body").Value;
                    var traceparentRv = e.Values.FirstOrDefault(v => v.Name == "traceparent").Value;
                    var tracestateRv  = e.Values.FirstOrDefault(v => v.Name == "tracestate").Value;

                    var bodyJson    = bodyRv.HasValue ? bodyRv.ToString() : string.Empty;
                    var traceparent = traceparentRv.HasValue ? traceparentRv.ToString() : string.Empty;
                    var tracestate  = tracestateRv.HasValue ? tracestateRv.ToString() : string.Empty;

                    // Extract parent from Publisher
                    var carrier = new Dictionary<string, string?>
                    {
                        ["traceparent"] = traceparent,
                        ["tracestate"]  = tracestate
                    };
                    var parentCtx = Propagator.Extract(default, carrier,
                        (c, key) => c.TryGetValue(key, out var v) && v != null ? new[] { v } : Array.Empty<string>());

                    using var activity = ActivitySource.StartActivity(
                        "PersistArticle",
                        ActivityKind.Consumer,
                        parentCtx.ActivityContext);

                    Serilog.Log.Information("ArticleService.Worker: consuming entry {EntryId} traceId={TraceId}", e.Id, (activity?.TraceId.ToString() ?? parentCtx.ActivityContext.TraceId.ToString()));

                    try
                    {
                        var evt = JsonSerializer.Deserialize<ArticlePublished>(bodyJson, jsonOpts)
                                  ?? throw new InvalidOperationException("Invalid message body");

                        var entity = new Article
                        {
                            Id          = evt.Id,
                            Title       = evt.Title,
                            Content     = evt.Content,
                            ShardKey    = evt.Region,
                            PublishedAt = evt.PublishedAt
                        };

                        await using var ctx = CreateCtx(evt.Region);
                        ctx.Entry(entity).State = EntityState.Added;
                        await ctx.SaveChangesAsync(stoppingToken);

                        // Cache warmup
                        static string AKey(string r, Guid id) => $"article:{r}:{id}";
                        static string RecentKey(string r)     => $"articles:recent:{r}";
                        var cache = _redis.GetDatabase();
                        var json = JsonSerializer.Serialize(entity, new JsonSerializerOptions(JsonSerializerDefaults.Web));

                        await cache.StringSetAsync(AKey(entity.ShardKey, entity.Id), json, TimeSpan.FromDays(15));
                        await cache.SortedSetAddAsync(RecentKey(entity.ShardKey), entity.Id.ToString(), entity.PublishedAt.ToUnixTimeSeconds());

                        await db.StreamAcknowledgeAsync(stream, "articlesvc", e.Id);
                    }
                    catch (Exception ex)
                    {
                        _log.Error(ex, "Failed to persist entry {Id} from {Stream}", e.Id, (string)stream);
                    }
                }
            }

            await Task.Delay(300, stoppingToken);
        }
    }

    private ArticleDbContext CreateCtx(string region)
    {
        var shards = _cfg.GetSection("Shards:ConnectionStrings").Get<Dictionary<string, string>>() ?? new();
        if (!shards.TryGetValue(region, out var cs))
            throw new InvalidOperationException($"No shard connection string for '{region}'");

        var opts = new DbContextOptionsBuilder<ArticleDbContext>()
            .UseNpgsql(cs)
            .Options;

        return new ArticleDbContext(opts);
    }

    private sealed record ArticlePublished(Guid Id, string Title, string Content, string Region, DateTimeOffset PublishedAt);
}
