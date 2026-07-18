using System.Text.Json;

sealed class McpStdioServer(FleetHost fleet)
{
    private static readonly JsonSerializerOptions Json = new() { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

    public async Task RunAsync()
    {
        string? line;
        while ((line = await Console.In.ReadLineAsync()) is not null)
        {
            try
            {
                using var document = JsonDocument.Parse(line);
                var request = document.RootElement;
                if (!request.TryGetProperty("method", out var methodNode)) continue;
                var method = methodNode.GetString();
                if (!request.TryGetProperty("id", out var id)) continue; // notifications have no response
                object result = method switch
                {
                    "initialize" => new
                    {
                        protocolVersion = "2025-03-26",
                        capabilities = new { tools = new { } },
                        serverInfo = new { name = "KimiFleet", version = "0.1.0" },
                        instructions = "Use KimiFleet tools to create scoped Kimi K3 agents, assign work, and require peer review."
                    },
                    "tools/list" => new { tools = Tools },
                    "tools/call" => await CallToolAsync(request.GetProperty("params")),
                    _ => throw new InvalidOperationException($"Unsupported MCP method '{method}'.")
                };
                await SendAsync(id, result);
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[mcp] {ex.Message}");
                await Console.Out.WriteLineAsync(JsonSerializer.Serialize(new { jsonrpc = "2.0", error = new { code = -32603, message = ex.Message } }, Json));
                await Console.Out.FlushAsync();
            }
        }
    }

    private async Task<object> CallToolAsync(JsonElement call)
    {
        var name = call.GetProperty("name").GetString() ?? throw new InvalidOperationException("Tool call has no name.");
        var arguments = call.TryGetProperty("arguments", out var value) ? value : default;
        object payload = name switch
        {
            "fleet_list_agents" => fleet.ListAgents(),
            "fleet_start_agent" => await fleet.StartAgentAsync(new FleetHost.AgentSpec(
                Required(arguments, "name"), Required(arguments, "workspace"),
                Optional(arguments, "role"), StringArray(arguments, "scopes"))),
            "fleet_assign_task" => Assign(Required(arguments, "agent"), Required(arguments, "prompt")),
            "fleet_request_review" => Review(Required(arguments, "reviewer"), Required(arguments, "author"), Optional(arguments, "context")),
            "fleet_stop_agent" => Stop(Required(arguments, "agent")),
            _ => throw new InvalidOperationException($"Unknown KimiFleet tool '{name}'.")
        };
        return new { content = new[] { new { type = "text", text = JsonSerializer.Serialize(payload, Json) } } };
    }

    private object Assign(string agent, string prompt)
    {
        fleet.Assign(agent, prompt);
        return new { accepted = true, agent };
    }

    private object Review(string reviewer, string author, string? context)
    {
        fleet.Review(reviewer, author, context);
        return new { accepted = true, reviewer, author };
    }

    private object Stop(string agent)
    {
        fleet.Stop(agent);
        return new { stopped = agent };
    }

    private static string Required(JsonElement input, string name) =>
        input.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.String && !string.IsNullOrWhiteSpace(value.GetString())
            ? value.GetString()! : throw new InvalidOperationException($"'{name}' is required.");

    private static string? Optional(JsonElement input, string name) =>
        input.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.String ? value.GetString() : null;

    private static string[]? StringArray(JsonElement input, string name) =>
        input.TryGetProperty(name, out var values) && values.ValueKind == JsonValueKind.Array
            ? values.EnumerateArray().Where(x => x.ValueKind == JsonValueKind.String).Select(x => x.GetString()!).ToArray() : null;

    private static async Task SendAsync(JsonElement id, object result)
    {
        var json = $"{{\"jsonrpc\":\"2.0\",\"id\":{id.GetRawText()},\"result\":{JsonSerializer.Serialize(result, Json)}}}";
        await Console.Out.WriteLineAsync(json);
        await Console.Out.FlushAsync();
    }

    private static readonly object[] Tools =
    [
        Tool("fleet_list_agents", "List active Kimi K3 agents, their roles, scope locks and state.", new { type = "object", properties = new { } }),
        Tool("fleet_start_agent", "Start a yolo Kimi K3 ACP agent with exclusive source scopes.", new { type = "object", required = new[] { "name", "workspace" }, properties = new { name = new { type = "string" }, workspace = new { type = "string" }, role = new { type = "string" }, scopes = new { type = "array", items = new { type = "string" } } } }),
        Tool("fleet_assign_task", "Assign an autonomous task to an active Kimi agent.", new { type = "object", required = new[] { "agent", "prompt" }, properties = new { agent = new { type = "string" }, prompt = new { type = "string" } } }),
        Tool("fleet_request_review", "Ask a second agent for read-only peer review of an author's current diff.", new { type = "object", required = new[] { "reviewer", "author" }, properties = new { reviewer = new { type = "string" }, author = new { type = "string" }, context = new { type = "string" } } }),
        Tool("fleet_stop_agent", "Stop an agent and release its exclusive scopes.", new { type = "object", required = new[] { "agent" }, properties = new { agent = new { type = "string" } } })
    ];

    private static object Tool(string name, string description, object inputSchema) => new { name, description, inputSchema };
}
