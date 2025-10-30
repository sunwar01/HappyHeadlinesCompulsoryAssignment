using CommentService.Cache;
using CommentService.Data;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using Prometheus;
using Serilog;
using Shared;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

// Same Serilog hook as ArticleService
builder.Host.UseSerilog((ctx, cfg) => cfg.ReadFrom.Configuration(ctx.Configuration));

builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();



// Redis connection (single place)
var redisConn = Environment.GetEnvironmentVariable("REDIS__CONNECTION") ?? "redis:6379";
builder.Services.AddSingleton<IConnectionMultiplexer>(_ => ConnectionMultiplexer.Connect(redisConn));

// Register Redis-based comment cache (TTL 30m, capacity 30 articleIds)
builder.Services.AddSingleton<ICommentCache>(sp =>
{
    var mux = sp.GetRequiredService<IConnectionMultiplexer>();
    return new RedisCommentCache(mux, TimeSpan.FromMinutes(30), capacity: 30);
});

var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI();
app.UseSerilogRequestLogging();
app.UseHttpMetrics(); 
app.UseMetricServer();
app.MapControllers();
app.MapMetrics();  

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

async Task EnsureSchemasAsync()
{
    var log = MonitorService.Log.ForContext("component", "CommentSchemaInit");
    var map = app.Configuration.GetSection("Shards:ConnectionStrings")
        .Get<Dictionary<string, string>>() ?? new();

    log.Information("Schema init: found {Count} shard entries: {Keys}", map.Count, string.Join(", ", map.Keys));

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

if (Environment.GetEnvironmentVariable("INIT_SCHEMA")?.Equals("true", StringComparison.OrdinalIgnoreCase) == true)
{
    await EnsureSchemasAsync();
}

app.Run();
