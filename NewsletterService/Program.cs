// Program.cs in NewsletterService

using NewsletterService.Background;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Prometheus;
using Serilog;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

// Use Serilog
Log.Logger = new LoggerConfiguration()
    .ReadFrom.Configuration(builder.Configuration)
    .Enrich.FromLogContext()
    .WriteTo.Console()
    .CreateBootstrapLogger();

builder.Host.UseSerilog();

// Register Serilog.ILogger as singleton
builder.Services.AddSingleton<Serilog.ILogger>(Log.Logger);

var redisConn = builder.Configuration["REDIS__CONNECTION"] ?? "redis:6379";
builder.Services.AddSingleton<IConnectionMultiplexer>(_ => 
    ConnectionMultiplexer.Connect(redisConn));

builder.Services.AddHostedService<SubscriberQueueWorker>();

builder.Services.AddOpenTelemetry()
    .ConfigureResource(r => r.AddService("NewsletterService"))
    .WithTracing(t => t
        .AddAspNetCoreInstrumentation()
        .AddRedisInstrumentation()
        .AddOtlpExporter());


var app = builder.Build();

app.UseMetricServer();   
app.MapMetrics();

app.MapGet("/health", () => Results.Ok());
app.Run();