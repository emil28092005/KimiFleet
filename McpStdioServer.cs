using System.Text.Json;

sealed class McpStdioServer(FleetHost fleet)
{
    private static readonly JsonSerializerOptions Json = new() { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

    public async Task RunAsync()
    {
        string? line;
        while ((line = await Console.In.ReadLineAsync()) is not null)
        {
            var responseId = "null";
            try
            {
                using var document = JsonDocument.Parse(line);
                var request = document.RootElement;
                if (!request.TryGetProperty("method", out var methodNode)) continue;
                var method = methodNode.GetString();
                if (!request.TryGetProperty("id", out var id)) continue; // notifications have no response
                responseId = id.GetRawText();
                object result = method switch
                {
                    "initialize" => new
                    {
                        protocolVersion = "2025-03-26",
                        capabilities = new { tools = new { } },
                        serverInfo = new { name = "KimiFleet", version = "0.1.0" },
                        instructions = "Use KimiFleet tools to create scoped Kimi K3 agents, assign work, and require peer review."
                    },
                    "ping" => new { },
                    "tools/list" => new { tools = Tools },
                    "tools/call" => await CallToolAsync(request.GetProperty("params")),
                    _ => throw new InvalidOperationException($"Unsupported MCP method '{method}'.")
                };
                await SendAsync(id, result);
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[mcp] {ex.Message}");
                await Console.Out.WriteLineAsync($"{{\"jsonrpc\":\"2.0\",\"id\":{responseId},\"error\":{JsonSerializer.Serialize(new { code = -32603, message = ex.Message }, Json)}}}");
                await Console.Out.FlushAsync();
            }
        }
    }

    private async Task<object> CallToolAsync(JsonElement call)
    {
        try
        {
            var name = call.GetProperty("name").GetString() ?? throw new InvalidOperationException("Tool call has no name.");
            var arguments = call.TryGetProperty("arguments", out var value) ? value : default;
            object payload = name switch
            {
                "fleet_list_agents" => fleet.ListAgents(),
                "fleet_start_agent" => await fleet.StartAgentAsync(new FleetHost.AgentSpec(
                    Required(arguments, "name"), Required(arguments, "workspace"),
                    Optional(arguments, "role"), StringArray(arguments, "scopes"))),
                "fleet_get_chat" => fleet.GetChat(Required(arguments, "agent")),
                "fleet_assign_task" => Assign(Required(arguments, "agent"), Required(arguments, "prompt")),
                "fleet_cancel_task" => await Cancel(Required(arguments, "agent")),
                "fleet_compact_context" => Compact(Required(arguments, "agent"), Optional(arguments, "instructions")),
                "fleet_reset_context" => await Reset(Required(arguments, "agent")),
                "fleet_request_review" => Review(Required(arguments, "reviewer"), Required(arguments, "author"), Optional(arguments, "context")),
                "fleet_handoff_context" => fleet.BeginHandoff(
                    Required(arguments, "from"), RequiredArray(arguments, "recipients"), Required(arguments, "kind"),
                    Optional(arguments, "title"), Optional(arguments, "instructions"), Optional(arguments, "message")),
                "fleet_list_handoffs" => fleet.ListHandoffs(Optional(arguments, "agent")),
                "fleet_stop_agent" => Stop(Required(arguments, "agent")),
                _ => throw new InvalidOperationException($"Unknown KimiFleet tool '{name}'.")
            };
            return new { content = new[] { new { type = "text", text = JsonSerializer.Serialize(payload, Json) } } };
        }
        catch (Exception ex)
        {
            return new { content = new[] { new { type = "text", text = ex.Message } }, isError = true };
        }
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

    private object Compact(string agent, string? instructions)
    {
        fleet.Compact(agent, instructions);
        return new { accepted = true, agent, operation = "compact" };
    }

    private async Task<object> Cancel(string agent)
    {
        await fleet.Cancel(agent);
        return new { accepted = true, agent, operation = "cancel" };
    }

    private async Task<object> Reset(string agent)
    {
        await fleet.ResetContext(agent);
        return new { accepted = true, agent, operation = "reset" };
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

    private static string[] RequiredArray(JsonElement input, string name)
    {
        var values = StringArray(input, name);
        return values is { Length: > 0 } ? values : throw new InvalidOperationException($"'{name}' must be a non-empty array of agent names.");
    }

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
        Tool("fleet_get_chat", "Get the complete visible chat and tool-call transcript for an agent.", new { type = "object", required = new[] { "agent" }, properties = new { agent = new { type = "string" } } }),
        Tool("fleet_assign_task", "Assign an autonomous task to an active Kimi agent.", new { type = "object", required = new[] { "agent", "prompt" }, properties = new { agent = new { type = "string" }, prompt = new { type = "string" } } }),
        Tool("fleet_cancel_task", "Cancel the task currently running in a Kimi agent.", new { type = "object", required = new[] { "agent" }, properties = new { agent = new { type = "string" } } }),
        Tool("fleet_compact_context", "Compact an agent context while preserving important decisions and active work.", new { type = "object", required = new[] { "agent" }, properties = new { agent = new { type = "string" }, instructions = new { type = "string" } } }),
        Tool("fleet_reset_context", "Create a fresh ACP context for an idle agent without changing files or scope ownership.", new { type = "object", required = new[] { "agent" }, properties = new { agent = new { type = "string" } } }),
        Tool("fleet_request_review", "Ask a second agent for read-only peer review of an author's current diff.", new { type = "object", required = new[] { "reviewer", "author" }, properties = new { reviewer = new { type = "string" }, author = new { type = "string" }, context = new { type = "string" } } }),
        Tool("fleet_stop_agent", "Stop an agent and release its exclusive scopes.", new { type = "object", required = new[] { "agent" }, properties = new { agent = new { type = "string" } } }),
        Tool("fleet_handoff_context", "Send agent-to-agent context over the fleet bus and return the accepted handoff record. kind 'message' delivers trimmed text without source summarization. For 'context', 'delegate', 'review' and 'broadcast', the source Kimi summarizes its live session into a structured package and KimiFleet injects it into every recipient's model context, consuming their token budgets.", new { type = "object", required = new[] { "from", "recipients", "kind" }, properties = new { from = new { type = "string", description = "Name of the sending agent." }, recipients = new { type = "array", minItems = 1, maxItems = 32, items = new { type = "string" }, description = "Names of the agents that receive the handoff." }, kind = new { type = "string", @enum = new[] { "message", "context", "delegate", "review", "broadcast" }, description = "'message' = direct text; other kinds = source-generated context package injected into recipient contexts." }, title = new { type = "string", maxLength = 200, description = "Short subject; labels the handoff and generated package." }, instructions = new { type = "string", maxLength = 8000, description = "Guidance for the source summary and the recipients' next action." }, message = new { type = "string", maxLength = 48000, description = "Required text for kind 'message' (48k max); optional extra detail for generated kinds (16k max)." } } }),
        Tool("fleet_list_handoffs", "List handoffs recorded on the fleet bus; pass 'agent' to keep only handoffs sent by or addressed to that agent.", new { type = "object", properties = new { agent = new { type = "string" } } })
    ];

    private static object Tool(string name, string description, object inputSchema) => new { name, description, inputSchema };
}
