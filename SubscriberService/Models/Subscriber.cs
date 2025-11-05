namespace SubscriberService.Models;

public class Subscriber
{
    public Guid Id { get; set; }
    public string Email { get; set; } = null!;
    public DateTimeOffset SubscribedAt { get; set; }
    public DateTimeOffset? UnsubscribedAt { get; set; }
    public string Region { get; set; } = null!;
}