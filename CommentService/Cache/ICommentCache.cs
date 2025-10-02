using CommentService.Models;

namespace CommentService.Cache;

public interface ICommentCache
{
    Task<(bool found, List<Comment>? comments)> TryGetAsync(Guid articleId);
    Task PutAsync(Guid articleId, List<Comment> comments);
    Task InvalidateAsync(Guid articleId);
}