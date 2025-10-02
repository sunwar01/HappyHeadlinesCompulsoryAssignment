namespace ArticleService.Data;


public static class Shards
{
    public static string PickConnection(string region, ShardMap map)
    {
        if (string.IsNullOrWhiteSpace(region)) region = "global";
        region = Normalize(region);

        if (!map.ConnectionStrings.TryGetValue(region, out var cs) &&
            !map.ConnectionStrings.TryGetValue("global", out cs))
        {
            throw new InvalidOperationException($"No connection string configured for region '{region}' or 'global'.");
        }
        return cs;

        static string Normalize(string c)
        {
            c = c.Trim().ToLowerInvariant().Replace(" ", "-").Replace("_", "-");
            return c switch
            {
                "na" => "north-america",
                "sa" => "south-america",
                "eu" => "europe",
                "oceania" => "australia",
                "global" => "global",
                _ => c
            };
        }
    }
}