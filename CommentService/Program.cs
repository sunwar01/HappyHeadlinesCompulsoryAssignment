using CommentService.Cache;
using CommentService.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Http.Resilience;
using Npgsql;
using Polly;

using Prometheus;
using Serilog;
using Shared;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

builder.Host.UseSerilog((ctx, cfg) => cfg.ReadFrom.Configuration(ctx.Configuration));

builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var redisConn = Environment.GetEnvironmentVariable("REDIS__CONNECTION") ?? "redis:6379";
builder.Services.AddSingleton<IConnectionMultiplexer>(_ => ConnectionMultiplexer.Connect(redisConn));

builder.Services.AddSingleton<ICommentCache>(sp =>
{
    var mux = sp.GetRequiredService<IConnectionMultiplexer>();
    return new RedisCommentCache(mux, TimeSpan.FromMinutes(30), capacity: 30);
});


builder.Services.AddHttpClient("ProfanityClient", client =>
    {
        client.BaseAddress = new Uri("http://profanity-service:8080");
        client.Timeout = Timeout.InfiniteTimeSpan;   // 👈 rely on Polly's timeout instead
    })
    .AddResilienceHandler("ProfanityPipeline", pipeline =>
    {
        pipeline.AddRetry(new() { MaxRetryAttempts = 3 });

        pipeline.AddCircuitBreaker(new HttpCircuitBreakerStrategyOptions
        {
            FailureRatio = 0.5,
            SamplingDuration = TimeSpan.FromSeconds(10),
            MinimumThroughput = 2,
            BreakDuration = TimeSpan.FromSeconds(30)
        });

        pipeline.AddTimeout(TimeSpan.FromSeconds(5));
    });

var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI();
app.UseSerilogRequestLogging();
app.UseHttpMetrics(); 
app.UseMetricServer();
app.MapControllers();
app.MapMetrics();  
app.MapGet("/health", () => Results.Ok(new { status = "ok" }));

// ---- Strong, explicit schema creation across all shards (only when INIT_SCHEMA=true) ----
const string ddl = """
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

if (Environment.GetEnvironmentVariable("INIT_SCHEMA")?.Equals("true", StringComparison.OrdinalIgnoreCase) == true)
{
    async Task EnsureSchemasAsync()
    {
        var log = MonitorService.Log.ForContext("component", "CommentSchemaInit");
        var map = app.Configuration.GetSection("Shards:ConnectionStrings")
            .Get<Dictionary<string, string>>() ?? new();

        log.Information("Comment schema init: found {Count} shard entries: {Keys}", map.Count, string.Join(", ", map.Keys));

        foreach (var (name, cs) in map)
        {
            try
            {
                await using var conn = new NpgsqlConnection(cs);
                await conn.OpenAsync();
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = ddl;
                await cmd.ExecuteNonQueryAsync();
                log.Information("Ensured public.\"Comments\" on shard {Shard}", name);
            }
            catch (Exception ex)
            {
                log.Error(ex, "Failed ensuring Comments schema on shard {Shard}", name);
            }
        }
    }

    await EnsureSchemasAsync();
}

app.Run();