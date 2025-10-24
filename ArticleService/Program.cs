using ArticleService.Background;
using ArticleService.Cache;
using ArticleService.Data;
using ArticleService.Models;
using Microsoft.EntityFrameworkCore;
using Prometheus;   
using Serilog;
using Shared;
using StackExchange.Redis;


var builder = WebApplication.CreateBuilder(args);



builder.Host.UseSerilog((ctx, cfg) => cfg
    .ReadFrom.Configuration(ctx.Configuration)
);


builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();



var redisConn = Environment.GetEnvironmentVariable("REDIS__CONNECTION") ?? "redis:6379";
builder.Services.AddSingleton<IConnectionMultiplexer>(_ => ConnectionMultiplexer.Connect(redisConn));

builder.Services.AddSingleton<IArticleCache>(sp =>
{
    var mux = sp.GetRequiredService<IConnectionMultiplexer>();
    return new RedisArticleCache(mux, TimeSpan.FromMinutes(30));
});





var app = builder.Build();

_ = MonitorService.Log;


app.UseSwagger();
app.UseSwaggerUI();





app.UseSerilogRequestLogging();
app.UseHttpMetrics();   
app.MapControllers();
app.MapMetrics();  
app.MapGet("/health", () => Results.Ok(new { status = "ok" }));

void EnsureSchemas(WebApplication app)
{
    var shards = app.Configuration.GetSection("Shards:ConnectionStrings")
        .Get<Dictionary<string,string>>() ?? new();
    foreach (var (_, cs) in shards)
    {
        var opts = new DbContextOptionsBuilder<ArticleDbContext>()
            .UseNpgsql(cs).Options;
        using var db = new ArticleService.Data.ArticleDbContext(opts);
        db.Database.EnsureCreated();
    }
}

EnsureSchemas(app);


app.Run();