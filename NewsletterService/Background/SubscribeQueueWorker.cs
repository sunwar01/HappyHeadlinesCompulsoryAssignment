using System.Text.Json;
using StackExchange.Redis;
using SubscriberService.Models;
using Serilog;


namespace NewsletterService.Background;

public class SubscriberQueueWorker : BackgroundService
{
    private readonly IConnectionMultiplexer _redis;
    private readonly Serilog.ILogger _log;

    public SubscriberQueueWorker(IConnectionMultiplexer redis, Serilog.ILogger log)
    {
        _redis = redis;
        _log = log.ForContext<SubscriberQueueWorker>(); 
    }
    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var db = _redis.GetDatabase();
        var regions = new[] { "global", "europe", "asia", "africa", "northamerica", "southamerica", "australia", "antarctica" };

        while (!stoppingToken.IsCancellationRequested)
        {
            bool processed = false;
            foreach (var region in regions)
            {
                var payload = await db.ListLeftPopAsync($"subscriber_queue:{region}");
                if (payload.HasValue)
                {
                    try
                    {
                        var sub = JsonSerializer.Deserialize<Subscriber>(payload!, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
                        if (sub != null)
                        {
                            _log.Information("Sending welcome email to {Email} ({Region})", sub.Email, region);
                            //TODO: Send email 
                        }
                        processed = true;
                    }
                    catch (Exception ex)
                    {
                        _log.Error(ex, "Failed to process subscriber from queue");
                    }
                }
            }

            if (!processed)
                await Task.Delay(500, stoppingToken);
        }
    }
}