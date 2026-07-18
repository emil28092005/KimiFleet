sealed class AgentChatEntry
{
    public required string Id { get; init; }
    public required DateTimeOffset Time { get; init; }
    public required string Type { get; init; }
    public string Text { get; set; } = string.Empty;
    public string? Title { get; set; }
    public string? Status { get; set; }
    public string? ToolKind { get; set; }
    public string? ToolCallId { get; set; }

    public object Snapshot() => new
    {
        id = Id,
        time = Time,
        type = Type,
        text = Text,
        title = Title,
        status = Status,
        toolKind = ToolKind,
        toolCallId = ToolCallId
    };
}
