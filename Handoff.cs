sealed class Handoff
{
    public static readonly string[] Kinds = ["message", "context", "delegate", "review", "broadcast"];
    public const int HistoryLimit = 100;
    public const int MaxInFlight = 128;

    public required string Id { get; init; }
    public required DateTimeOffset Time { get; init; }
    public required string From { get; init; }
    public required string[] Recipients { get; init; }
    public required string Kind { get; init; }
    public string? Title { get; init; }
    public string? Instructions { get; init; }
    public string? Message { get; init; }
    public string Status { get; set; } = "preparing";
    public string? Error { get; set; }
    public string? Package { get; set; }
    public List<HandoffDelivery> Deliveries { get; } = [];

    public object Snapshot() => new
    {
        id = Id,
        createdAt = Time,
        from = From,
        recipients = Recipients,
        kind = Kind,
        title = Title,
        instructions = Instructions,
        message = Message,
        status = Status,
        error = Error,
        content = Package,
        deliveries = Deliveries.Select(d => d.Snapshot()).ToArray()
    };
}

sealed class HandoffDelivery
{
    public required string Agent { get; init; }
    public string State { get; set; } = "pending";
    public string? Error { get; set; }

    public object Snapshot() => new { agent = Agent, status = State, error = Error };
}
