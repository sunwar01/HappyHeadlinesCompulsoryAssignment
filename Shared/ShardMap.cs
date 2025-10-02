namespace CommentService.Data;

public class ShardMap
{
    public Dictionary<string,string> ConnectionStrings { get; set; } = new(StringComparer.OrdinalIgnoreCase);
}