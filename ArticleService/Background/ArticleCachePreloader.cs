using ArticleService.Cache;
using ArticleService.Data;
using Microsoft.EntityFrameworkCore;
using Prometheus;

namespace ArticleService.Background;

public sealed class ArticleCachePreloader : BackgroundService
{
    private readonly IServiceProvider _sp;
    private readonly TimeSpan _period;
    private readonly TimeSpan _ttl;

    private static readonly Counter Runs  = Metrics.CreateCounter("article_cache_prefill_runs_total", "Prefill runs");
    private static readonly Counter Items = Metrics.CreateCounter("article_cache_prefill_items_total", "Items cached");

    public ArticleCachePreloader(IServiceProvider sp, TimeSpan period, TimeSpan ttl)
        => (_sp, _period, _ttl) = (sp, period, ttl);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                using var scope = _sp.CreateScope();
                var db = scope.ServiceProvider.GetRequiredService<ArticleDbContext>();
                var cache = scope.ServiceProvider.GetRequiredService<IArticleCache>();

                var cutoff = DateTimeOffset.UtcNow.AddDays(-14);
                var recent = await db.Articles.AsNoTracking()
                    .Where(a => a.PublishedAt >= cutoff)
                    .ToListAsync(stoppingToken);

                var count = await cache.SetManyAsync(recent, _ttl);
                Runs.Inc();
                Items.Inc(count);
            }
            catch {  }

            await Task.Delay(_period, stoppingToken);
        }
    }
}