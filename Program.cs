using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Channels;

var options = FleetOptions.Parse(args);
var fleet = new FleetHost(options);
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    fleet.Dispose();
    Environment.Exit(0);
};

Console.Error.WriteLine($"KimiFleet dashboard: http://127.0.0.1:{options.Port}");
var webTask = fleet.RunAsync();
if (options.EnableMcp)
    await Task.WhenAny(webTask, new McpStdioServer(fleet).RunAsync());
else
    await webTask;

sealed record FleetOptions(int Port, string KimiExecutable, string Model, bool EnableMcp)
{
    public static FleetOptions Parse(string[] args)
    {
        var port = 7373;
        var kimi = "kimi";
        var model = "kimi-code/k3";
        var enableMcp = false;
        for (var i = 0; i < args.Length; i++)
        {
            if (args[i] == "--port" && i + 1 < args.Length && int.TryParse(args[++i], out var value)) port = value;
            else if (args[i] == "--kimi" && i + 1 < args.Length) kimi = args[++i];
            else if (args[i] == "--model" && i + 1 < args.Length) model = args[++i];
            else if (args[i] == "--mcp") enableMcp = true;
        }
        return new FleetOptions(port, kimi, model, enableMcp);
    }
}

sealed class FleetHost : IDisposable
{
    private readonly FleetOptions _options;
    private readonly HttpListener _listener = new();
    private readonly ConcurrentDictionary<string, KimiAgent> _agents = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, string> _scopeOwners = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<Guid, Channel<string>> _eventSubscribers = new();
    private readonly List<string> _recentEvents = [];
    private readonly object _eventsLock = new();
    private bool _disposed;

    public FleetHost(FleetOptions options)
    {
        _options = options;
        _listener.Prefixes.Add($"http://127.0.0.1:{options.Port}/");
    }

    public async Task RunAsync()
    {
        _listener.Start();
        while (!_disposed)
        {
            HttpListenerContext context;
            try { context = await _listener.GetContextAsync(); }
            catch (ObjectDisposedException) { break; }
            _ = Task.Run(() => HandleAsync(context));
        }
    }

    private async Task HandleAsync(HttpListenerContext context)
    {
        try
        {
            var path = context.Request.Url?.AbsolutePath.Trim('/') ?? string.Empty;
            var parts = path.Split('/', StringSplitOptions.RemoveEmptyEntries);
            if (context.Request.HttpMethod == "GET" && path.Length == 0)
                await ServeDashboardAsync(context);
            else if (context.Request.HttpMethod == "GET" && path == "health")
                await RespondJsonAsync(context, new { ok = true, model = _options.Model });
            else if (context.Request.HttpMethod == "GET" && path == "agents")
                await RespondJsonAsync(context, _agents.Values.Select(a => a.Snapshot()));
            else if (context.Request.HttpMethod == "GET" && path == "events")
                await StreamEventsAsync(context);
            else if (context.Request.HttpMethod == "POST" && path == "agents")
                await StartAgentAsync(context);
            else if (context.Request.HttpMethod == "POST" && parts.Length == 3 && parts[0] == "agents" && parts[2] == "prompt")
                await PromptAgentAsync(context, parts[1]);
            else if (context.Request.HttpMethod == "POST" && parts.Length == 3 && parts[0] == "agents" && parts[2] == "review")
                await ReviewAsync(context, parts[1]);
            else if (context.Request.HttpMethod == "POST" && parts.Length == 3 && parts[0] == "agents" && parts[2] == "stop")
                await StopAgentAsync(context, parts[1]);
            else
                await RespondJsonAsync(context, new { error = "Unknown endpoint." }, 404);
        }
        catch (Exception ex)
        {
            Publish("fleet", "error", ex.Message);
            if (context.Response.OutputStream.CanWrite)
                await RespondJsonAsync(context, new { error = ex.Message }, 500);
        }
    }

    private async Task StartAgentAsync(HttpListenerContext context)
    {
        var request = await ReadJsonAsync<StartAgentRequest>(context);
        var agent = await StartAgentAsync(new AgentSpec(request.Name, request.Workspace, request.Role, request.Scopes));
        await RespondJsonAsync(context, agent, 201);
    }

    private async Task PromptAgentAsync(HttpListenerContext context, string name)
    {
        var request = await ReadJsonAsync<PromptRequest>(context);
        Assign(name, request.Prompt);
        await RespondJsonAsync(context, new { accepted = true, agent = name });
    }

    private async Task ReviewAsync(HttpListenerContext context, string reviewerName)
    {
        var request = await ReadJsonAsync<ReviewRequest>(context);
        Review(reviewerName, request.Author, request.Context);
        await RespondJsonAsync(context, new { accepted = true, reviewer = reviewerName, author = request.Author });
    }

    private async Task StopAgentAsync(HttpListenerContext context, string name)
    {
        RemoveAgent(name);
        await RespondJsonAsync(context, new { stopped = name });
    }

    public async Task<object> StartAgentAsync(AgentSpec spec)
    {
        var name = ValidateName(spec.Name);
        var workspace = Path.GetFullPath(spec.Workspace);
        if (!Directory.Exists(workspace)) throw new InvalidOperationException($"Workspace does not exist: {workspace}");
        if (_agents.ContainsKey(name)) throw new InvalidOperationException($"Agent '{name}' already exists.");
        var scopes = spec.Scopes?.Distinct(StringComparer.OrdinalIgnoreCase).ToArray() ?? [];
        foreach (var scope in scopes)
            if (_scopeOwners.TryGetValue(scope, out var owner)) throw new InvalidOperationException($"Scope '{scope}' is owned by '{owner}'.");
        var agent = new KimiAgent(name, workspace, spec.Role ?? "worker", scopes, _options, Publish);
        if (!_agents.TryAdd(name, agent)) throw new InvalidOperationException($"Could not add '{name}'.");
        foreach (var scope in scopes) _scopeOwners[scope] = name;
        try { await agent.StartAsync(); return agent.Snapshot(); }
        catch { RemoveAgent(name); throw; }
    }

    public IReadOnlyList<object> ListAgents() => _agents.Values.Select(a => a.Snapshot()).Cast<object>().ToArray();
    public void Assign(string name, string prompt) => _ = GetAgent(name).PromptAsync(prompt);
    public void Review(string reviewerName, string authorName, string? context)
    {
        var reviewer = GetAgent(reviewerName);
        var author = GetAgent(authorName);
        _ = reviewer.PromptAsync($"Perform peer review for agent '{author.Name}'. Review only; do not edit files. Inspect current git diff and changed files. Check correctness, resource safety, tests, and scope ownership. Report findings by severity with file/line locations. Context: {context ?? "No additional context."}");
    }
    public void Stop(string name) => RemoveAgent(name);

    private KimiAgent GetAgent(string name) =>
        _agents.TryGetValue(name, out var agent) ? agent : throw new InvalidOperationException($"Unknown agent '{name}'.");

    private void RemoveAgent(string name)
    {
        if (!_agents.TryRemove(name, out var agent)) return;
        foreach (var scope in agent.Scopes) _scopeOwners.TryRemove(scope, out _);
        agent.Dispose();
        Publish(name, "stopped", "Agent stopped.");
    }

    private static string ValidateName(string? name)
    {
        if (string.IsNullOrWhiteSpace(name) || name.Any(c => !char.IsAsciiLetterOrDigit(c) && c is not '-' and not '_'))
            throw new InvalidOperationException("Agent name must contain only ASCII letters, digits, '-' or '_'.");
        return name;
    }

    private async Task<T> ReadJsonAsync<T>(HttpListenerContext context)
    {
        using var reader = new StreamReader(context.Request.InputStream, context.Request.ContentEncoding);
        var value = await JsonSerializer.DeserializeAsync<T>(reader.BaseStream, JsonOptions);
        return value ?? throw new InvalidOperationException("Request body must be valid JSON.");
    }

    private static async Task RespondJsonAsync(HttpListenerContext context, object body, int status = 200)
    {
        var bytes = JsonSerializer.SerializeToUtf8Bytes(body, JsonOptions);
        context.Response.StatusCode = status;
        context.Response.ContentType = "application/json; charset=utf-8";
        context.Response.ContentLength64 = bytes.Length;
        await context.Response.OutputStream.WriteAsync(bytes);
        context.Response.Close();
    }

    private static async Task ServeDashboardAsync(HttpListenerContext context)
    {
        var html = await File.ReadAllBytesAsync(Path.Combine(AppContext.BaseDirectory, "Dashboard.html"));
        context.Response.ContentType = "text/html; charset=utf-8";
        context.Response.ContentLength64 = html.Length;
        await context.Response.OutputStream.WriteAsync(html);
        context.Response.Close();
    }

    private async Task StreamEventsAsync(HttpListenerContext context)
    {
        context.Response.StatusCode = 200;
        context.Response.ContentType = "text/event-stream";
        context.Response.SendChunked = true;
        context.Response.Headers.Add("Cache-Control", "no-cache");
        var id = Guid.NewGuid();
        var channel = Channel.CreateUnbounded<string>();
        _eventSubscribers[id] = channel;
        try
        {
            string[] snapshot;
            lock (_eventsLock) snapshot = _recentEvents.ToArray();
            foreach (var line in snapshot) await WriteSseAsync(context.Response, line);
            await foreach (var line in channel.Reader.ReadAllAsync()) await WriteSseAsync(context.Response, line);
        }
        catch (HttpListenerException) { }
        catch (IOException) { }
        finally { _eventSubscribers.TryRemove(id, out _); context.Response.Close(); }
    }

    private static async Task WriteSseAsync(HttpListenerResponse response, string json)
    {
        var data = Encoding.UTF8.GetBytes($"data: {json}\n\n");
        await response.OutputStream.WriteAsync(data);
        await response.OutputStream.FlushAsync();
    }

    private void Publish(string agent, string kind, string message)
    {
        var json = JsonSerializer.Serialize(new { time = DateTimeOffset.UtcNow, agent, kind, message }, JsonOptions);
        lock (_eventsLock)
        {
            _recentEvents.Add(json);
            if (_recentEvents.Count > 250) _recentEvents.RemoveAt(0);
        }
        Console.Error.WriteLine($"[{DateTime.Now:HH:mm:ss}] [{agent}] {kind}: {DescribeEvent(kind, message)}");
        foreach (var subscriber in _eventSubscribers.Values) subscriber.Writer.TryWrite(json);
    }

    private static string DescribeEvent(string kind, string message)
    {
        if (kind != "session/update") return message;
        try
        {
            using var document = JsonDocument.Parse(message);
            var update = document.RootElement.GetProperty("update");
            var type = update.GetProperty("sessionUpdate").GetString();
            if (type == "tool_call_update") return $"{update.GetProperty("title").GetString()} — {update.GetProperty("status").GetString()}";
            if (type == "agent_message_chunk") return "Kimi sent a response";
            return type ?? "session update";
        }
        catch (JsonException) { return "session update"; }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _listener.Close();
        foreach (var name in _agents.Keys.ToArray()) RemoveAgent(name);
        foreach (var subscriber in _eventSubscribers.Values) subscriber.Writer.TryComplete();
    }

    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

    public sealed record AgentSpec(string Name, string Workspace, string? Role, string[]? Scopes);
    private sealed record StartAgentRequest(string Name, string Workspace, string? Role, string[]? Scopes);
    private sealed record PromptRequest(string Prompt);
    private sealed record ReviewRequest(string Author, string? Context);
}

sealed class KimiAgent : IDisposable
{
    private readonly FleetOptions _options;
    private readonly Action<string, string, string> _publish;
    private readonly Process _process;
    private readonly ConcurrentDictionary<long, TaskCompletionSource<JsonElement>> _pending = new();
    private readonly SemaphoreSlim _stdinLock = new(1, 1);
    private long _requestId;
    private string? _sessionId;
    private bool _disposed;

    public string Name { get; }
    public string Workspace { get; }
    public string Role { get; }
    public IReadOnlyList<string> Scopes { get; }
    public string State { get; private set; } = "starting";
    public string Activity { get; private set; } = "Starting ACP session";
    public DateTimeOffset ActivityUpdatedAt { get; private set; } = DateTimeOffset.UtcNow;

    public KimiAgent(string name, string workspace, string role, IReadOnlyList<string> scopes, FleetOptions options, Action<string, string, string> publish)
    {
        Name = name;
        Workspace = workspace;
        Role = role;
        Scopes = scopes;
        _options = options;
        _publish = publish;
        _process = new Process
        {
            StartInfo = new ProcessStartInfo
            {
                FileName = options.KimiExecutable,
                WorkingDirectory = workspace,
                RedirectStandardInput = true,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            },
            EnableRaisingEvents = true
        };
        _process.StartInfo.ArgumentList.Add("--yolo");
        _process.StartInfo.ArgumentList.Add("--model");
        _process.StartInfo.ArgumentList.Add(options.Model);
        _process.StartInfo.ArgumentList.Add("acp");
    }

    public async Task StartAsync()
    {
        SetActivity("Initializing ACP session");
        if (!_process.Start()) throw new InvalidOperationException($"Could not start Kimi process for '{Name}'.");
        _process.Exited += (_, _) => { State = "stopped"; _publish(Name, "exit", $"Kimi exited with {_process.ExitCode}."); };
        _ = ReadOutputAsync();
        _ = ReadErrorAsync();
        var initialized = await RequestAsync("initialize", new
        {
            protocolVersion = 1,
            clientCapabilities = new { fs = new { readTextFile = true, writeTextFile = true } },
            clientInfo = new { name = "KimiFleet", version = "0.1.0" }
        });
        _publish(Name, "initialized", initialized.GetProperty("agentInfo").GetProperty("name").GetString() ?? "Kimi Code CLI");
        var session = await RequestAsync("session/new", new { cwd = Workspace, mcpServers = Array.Empty<object>() });
        _sessionId = session.GetProperty("sessionId").GetString() ?? throw new InvalidOperationException("ACP did not return sessionId.");
        State = "ready";
        SetActivity("Ready for a task");
        _publish(Name, "ready", $"Role={Role}; scopes={string.Join(", ", Scopes)}");
    }

    public async Task PromptAsync(string prompt)
    {
        if (string.IsNullOrWhiteSpace(prompt)) throw new InvalidOperationException("Prompt must not be empty.");
        if (_sessionId is null) throw new InvalidOperationException($"Agent '{Name}' has no ACP session.");
        State = "working";
        SetActivity("Planning assigned task");
        _publish(Name, "prompt", prompt.Length > 160 ? prompt[..160] + "…" : prompt);
        try
        {
            await RequestAsync("session/prompt", new { sessionId = _sessionId, prompt = new[] { new { type = "text", text = prompt } } });
            State = "ready";
            SetActivity("Ready for a task");
            _publish(Name, "completed", "Prompt completed.");
        }
        catch (Exception ex)
        {
            State = "error";
            SetActivity("Needs attention");
            _publish(Name, "error", ex.Message);
        }
    }

    private async Task<JsonElement> RequestAsync(string method, object parameters)
    {
        var id = Interlocked.Increment(ref _requestId);
        var completion = new TaskCompletionSource<JsonElement>(TaskCreationOptions.RunContinuationsAsynchronously);
        if (!_pending.TryAdd(id, completion)) throw new InvalidOperationException("Could not register ACP request.");
        var envelope = JsonSerializer.Serialize(new { jsonrpc = "2.0", id, method, @params = parameters });
        await SendRawAsync(envelope);
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(10));
        await using var registration = timeout.Token.Register(() => completion.TrySetException(new TimeoutException($"ACP request '{method}' timed out.")));
        return await completion.Task;
    }

    private async Task ReadOutputAsync()
    {
        while (!_process.HasExited)
        {
            var line = await _process.StandardOutput.ReadLineAsync();
            if (line is null) break;
            HandleMessage(line);
        }
    }

    private async Task ReadErrorAsync()
    {
        while (!_process.HasExited)
        {
            var line = await _process.StandardError.ReadLineAsync();
            if (line is null) break;
            _publish(Name, "stderr", line);
        }
    }

    private void HandleMessage(string line)
    {
        try
        {
            using var document = JsonDocument.Parse(line);
            var root = document.RootElement;
            if (root.TryGetProperty("id", out var idNode) && idNode.TryGetInt64(out var id) && _pending.TryRemove(id, out var completion))
            {
                if (root.TryGetProperty("error", out var error)) completion.TrySetException(new InvalidOperationException(error.GetRawText()));
                else if (root.TryGetProperty("result", out var result)) completion.TrySetResult(result.Clone());
                else completion.TrySetException(new InvalidOperationException("ACP response has neither result nor error."));
                return;
            }
            if (root.TryGetProperty("method", out var method))
            {
                var methodName = method.GetString() ?? "notification";
                if (methodName == "session/request_permission" && root.TryGetProperty("id", out var permissionId) && root.TryGetProperty("params", out var permission))
                {
                    var optionId = SelectApproval(permission);
                    SetActivity("Approving requested action");
                    _ = RespondToPermissionAsync(permissionId.GetRawText(), optionId);
                    _publish(Name, "permission", $"Auto-approved '{optionId}' (yolo mode).");
                    return;
                }
                if (methodName == "session/update" && root.TryGetProperty("params", out var updateParams))
                    UpdateActivityFromSessionUpdate(updateParams);
                _publish(Name, methodName, root.TryGetProperty("params", out var p) ? p.GetRawText() : string.Empty);
            }
            else _publish(Name, "protocol", line);
        }
        catch (JsonException) { _publish(Name, "stdout", line); }
    }

    private void UpdateActivityFromSessionUpdate(JsonElement parameters)
    {
        if (!parameters.TryGetProperty("update", out var update) || !update.TryGetProperty("sessionUpdate", out var typeNode)) return;
        var type = typeNode.GetString();
        if ((type == "tool_call" || type == "tool_call_update") && update.TryGetProperty("title", out var title) && !string.IsNullOrWhiteSpace(title.GetString()))
            SetActivity(title.GetString()!);
        else if (type == "agent_thought_chunk" && !Activity.StartsWith("Reading", StringComparison.OrdinalIgnoreCase) && !Activity.StartsWith("Running", StringComparison.OrdinalIgnoreCase))
            SetActivity("Analyzing task");
        else if (type == "agent_message_chunk")
            SetActivity("Preparing a report");
    }

    private void SetActivity(string value)
    {
        Activity = value;
        ActivityUpdatedAt = DateTimeOffset.UtcNow;
    }

    private static string SelectApproval(JsonElement permission)
    {
        if (!permission.TryGetProperty("options", out var options) || options.ValueKind != JsonValueKind.Array)
            throw new InvalidOperationException("ACP permission request has no options.");
        var ids = options.EnumerateArray()
            .Where(option => option.TryGetProperty("optionId", out _))
            .Select(option => option.GetProperty("optionId").GetString())
            .Where(id => !string.IsNullOrWhiteSpace(id))
            .Cast<string>()
            .ToArray();
        return ids.FirstOrDefault(id => id.Contains("approve_always", StringComparison.OrdinalIgnoreCase))
            ?? ids.FirstOrDefault(id => id.Contains("approve", StringComparison.OrdinalIgnoreCase))
            ?? throw new InvalidOperationException("ACP permission request has no approval option.");
    }

    private Task RespondToPermissionAsync(string requestId, string optionId) =>
        SendRawAsync($"{{\"jsonrpc\":\"2.0\",\"id\":{requestId},\"result\":{{\"outcome\":{{\"outcome\":\"selected\",\"optionId\":{JsonSerializer.Serialize(optionId)}}}}}}}");

    private async Task SendRawAsync(string json)
    {
        await _stdinLock.WaitAsync();
        try
        {
            await _process.StandardInput.WriteLineAsync(json);
            await _process.StandardInput.FlushAsync();
        }
        finally { _stdinLock.Release(); }
    }

    public object Snapshot() => new { name = Name, workspace = Workspace, role = Role, scopes = Scopes, state = State, activity = Activity, activityUpdatedAt = ActivityUpdatedAt, sessionId = _sessionId };

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        try { if (!_process.HasExited) _process.Kill(entireProcessTree: true); } catch (InvalidOperationException) { }
        _stdinLock.Dispose();
        _process.Dispose();
    }
}
