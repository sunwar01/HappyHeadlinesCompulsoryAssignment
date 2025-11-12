// PublisherService/Program.cs
using System.Diagnostics;
using Microsoft.AspNetCore.Mvc;
using OpenTelemetry;
using OpenTelemetry.Context.Propagation;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Prometheus;
using PublisherService.Models;
using Serilog;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

// Serilog
builder.Host.UseSerilog((ctx, cfg) => cfg.ReadFrom.Configuration(ctx.Configuration));

// Force W3C propagation (Zipkin handles this fine)
Sdk.SetDefaultTextMapPropagator(new TraceContextPropagator());

// Redis
var redisConn = builder.Configuration["REDIS__CONNECTION"] ?? "redis:6379";
builder.Services.AddSingleton<IConnectionMultiplexer>(_ => ConnectionMultiplexer.Connect(redisConn));

// OpenTelemetry → Zipkin
builder.Services.AddOpenTelemetry()
    .ConfigureResource(r => r.AddService("PublisherService"))
    .WithTracing(t => t
        .AddAspNetCoreInstrumentation()
        .AddHttpClientInstrumentation()
        // Keep Redis instrumentation here if you want to see the XADD span in Zipkin
        .AddRedisInstrumentation()
        .AddSource("PublisherService.Messaging")
        .SetSampler(new AlwaysOnSampler())
        .AddZipkinExporter(o => o.Endpoint = new Uri("http://zipkin:9411/api/v2/spans"))
    );

// MVC + Swagger
builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

// Prometheus
app.UseHttpMetrics();
app.UseMetricServer();
app.MapMetrics();

// Swagger
app.UseSwagger();
app.UseSwaggerUI();

// Health
app.MapGet("/health", () => Results.Ok(new { status = "ok" }));

// Producer ActivitySource
var msgSource = new ActivitySource("PublisherService.Messaging");

// Publish endpoint
app.MapPost("/api/{region}/publish", async (
    [FromRoute] string region,
    [FromBody] PublishArticleDto dto,
    IConnectionMultiplexer mux,
    CancellationToken ct) =>
{
    region = region.ToLowerInvariant();

    var msg = new ArticlePublished(
        Id: Guid.NewGuid(),
        Title: dto.Title,
        Content: dto.Content,
        Region: region,
        PublishedAt: dto.PublishedAt ?? DateTimeOffset.UtcNow);

    // Start PRODUCER span (child of HTTP server span)
    using var producer = msgSource.StartActivity($"redis XADD article_queue:{region}", ActivityKind.Producer);

    // Inject W3C context from producer (fallback to current request if needed)
    var headers = new Dictionary<string, string>();
    var parentCtx = new PropagationContext(producer?.Context ?? Activity.Current?.Context ?? default, Baggage.Current);
    Propagators.DefaultTextMapPropagator.Inject(parentCtx, headers, (d, k, v) => d[k] = v);

    // Useful: log trace id so you can search in Zipkin
    Serilog.Log.Information("PublisherService: produced message {Id} traceId={TraceId}", msg.Id, parentCtx.ActivityContext.TraceId.ToString());

    var db = mux.GetDatabase();
    var stream = $"article_queue:{region}";

    var fields = new NameValueEntry[]
    {
        new("body", System.Text.Json.JsonSerializer.Serialize(msg)),
        new("traceparent", headers.TryGetValue("traceparent", out var tp) ? tp : string.Empty),
        new("tracestate",  headers.TryGetValue("tracestate",  out var ts) ? ts : string.Empty)
    };

    await db.StreamAddAsync(stream, fields);

    return Results.Accepted($"/api/{region}/articles/{msg.Id}", new { msg.Id });
})
.WithName("PublishArticle")
.Produces(StatusCodes.Status202Accepted)
.ProducesValidationProblem();

app.Run();

// Local event record
internal sealed record ArticlePublished(Guid Id, string Title, string Content, string Region, DateTimeOffset PublishedAt);
