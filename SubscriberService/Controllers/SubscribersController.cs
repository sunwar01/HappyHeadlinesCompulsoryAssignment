using System.Text.Json;
using ArticleService.Data;
using CommentService.Data;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Prometheus;
using Serilog;
using StackExchange.Redis;
using Shared;
using SubscriberService.Data;
using SubscriberService.Models;
using ILogger = Serilog.ILogger;

namespace SubscriberService.Controllers;

[ApiController]
[Route("api/{region}/[controller]")]
public class SubscribersController : ControllerBase
{
    private readonly ShardMap _shards;
    private readonly IDatabase _cache;
    private readonly IConfiguration _config;
    private static readonly ILogger _log = Log.ForContext<SubscribersController>();

    private static readonly Counter SubscribeTotal = Metrics.CreateCounter(
        "subscriber_subscribe_total", "Total subscribe attempts",
        new CounterConfiguration { LabelNames = new[] { "region", "status" } });

    private static readonly Counter UnsubscribeTotal = Metrics.CreateCounter(
        "subscriber_unsubscribe_total", "Total unsubscribe attempts",
        new CounterConfiguration { LabelNames = new[] { "region", "status" } });

    private static readonly Histogram RequestDuration = Metrics.CreateHistogram(
        "subscriber_request_duration_seconds",
        "Duration of HTTP requests in SubscriberService",
        new HistogramConfiguration
        {
            LabelNames = new[] { "region", "method", "endpoint", "status" },
            Buckets = Histogram.ExponentialBuckets(0.005, 2, 12)
        });

    public SubscribersController(IConfiguration config, IConnectionMultiplexer redis)
    {
        _config = config;
        _cache = redis.GetDatabase();
        _shards = new ShardMap
        {
            ConnectionStrings = config.GetSection("Shards:ConnectionStrings")
                                      .Get<Dictionary<string, string>>() ?? new()
        };
    }

    private SubscriberDbContext Ctx(string region)
    {
        var cs = Shards.PickConnection(region, _shards);
        var opts = new DbContextOptionsBuilder<SubscriberDbContext>()
            .UseNpgsql(cs).Options;
        return new SubscriberDbContext(opts);
    }

    private string QueueKey(string region) => $"subscriber_queue:{region}";

    private async Task<ActionResult<T>> Measure<T>(
        string endpoint,
        Func<CancellationToken, Task<ActionResult<T>>> work,
        CancellationToken ct = default)
    {
        var region = (string)RouteData.Values["region"]!;
        var method = HttpContext.Request.Method;
        var sw = System.Diagnostics.Stopwatch.StartNew();

        try
        {
            var result = await work(ct);
            var status = result.Result switch
            {
                ObjectResult obj => obj.StatusCode?.ToString() ?? "200",
                StatusCodeResult sc => sc.StatusCode.ToString(),
                _ => "200"
            };

            RequestDuration.WithLabels(region, method, endpoint, status)
                           .Observe(sw.Elapsed.TotalSeconds);
            return result;
        }
        catch (Exception ex) when (!(ex is OperationCanceledException))
        {
            RequestDuration.WithLabels(region, method, endpoint, "500")
                           .Observe(sw.Elapsed.TotalSeconds);
            _log.Error(ex, "Unhandled error in {Endpoint}", endpoint);
            return StatusCode(500, "Internal Server Error");
        }
    }

    private async Task<IActionResult> Measure(
        string endpoint,
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
            return result;
        }
        catch (Exception ex) when (!(ex is OperationCanceledException))
        {
            RequestDuration.WithLabels(region, method, endpoint, "500")
                           .Observe(sw.Elapsed.TotalSeconds);
            _log.Error(ex, "Unhandled error in {Endpoint}", endpoint);
            return StatusCode(500, "Internal Server Error");
        }
    }

    [HttpPost("subscribe")]
    public Task<ActionResult<Subscriber>> Subscribe(
        [FromRoute] string region,
        [FromBody] SubscribeDto dto,
        CancellationToken ct = default)
    {
        return Measure<Subscriber>("subscribe", async token =>
        {
            if (string.IsNullOrWhiteSpace(dto.Email) || !dto.Email.Contains('@'))
            {
                SubscribeTotal.WithLabels(region, "invalid").Inc();
                return BadRequest("Valid email required.");
            }

            await using var db = Ctx(region);
            var existing = await db.Subscribers
                .FirstOrDefaultAsync(s => s.Email == dto.Email && s.UnsubscribedAt == null, token);

            if (existing != null)
            {
                SubscribeTotal.WithLabels(region, "already").Inc();
                return Conflict("Already subscribed.");
            }

            var subscriber = new Subscriber
            {
                Id = Guid.NewGuid(),
                Email = dto.Email,
                SubscribedAt = DateTimeOffset.UtcNow,
                Region = region
            };

            db.Subscribers.Add(subscriber);
            await db.SaveChangesAsync(token);

            try
            {
                var json = JsonSerializer.Serialize(subscriber);
                await _cache.ListRightPushAsync(QueueKey(region), json);
                _log.Information("Enqueued subscriber {Email} in {Region}", dto.Email, region);
            }
            catch (Exception ex)
            {
                _log.Warning(ex, "Failed to enqueue subscriber {Email}", dto.Email);
            }

            SubscribeTotal.WithLabels(region, "success").Inc();
            return Created($"/api/{region}/subscribers/{subscriber.Id}", subscriber);
        }, ct);
    }

    [HttpPost("unsubscribe")]
    public Task<IActionResult> Unsubscribe(
        [FromRoute] string region,
        [FromBody] SubscribeDto dto,
        CancellationToken ct = default)
    {
        return Measure("unsubscribe", async token =>
        {
            if (string.IsNullOrWhiteSpace(dto.Email))
            {
                UnsubscribeTotal.WithLabels(region, "invalid").Inc();
                return BadRequest("Email required.");
            }

            await using var db = Ctx(region);
            var subscriber = await db.Subscribers
                .FirstOrDefaultAsync(s => s.Email == dto.Email && s.UnsubscribedAt == null, token);

            if (subscriber == null)
            {
                UnsubscribeTotal.WithLabels(region, "notfound").Inc();
                return NotFound();
            }

            subscriber.UnsubscribedAt = DateTimeOffset.UtcNow;
            await db.SaveChangesAsync(token);

            UnsubscribeTotal.WithLabels(region, "success").Inc();
            return NoContent();
        }, ct);
    }
}