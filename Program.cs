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

sealed record FleetOptions(int Port, string KimiExecutable, string Model, int MaxContextTokens, bool EnableMcp)
{
    public static FleetOptions Parse(string[] args)
    {
        var port = 7373;
        var kimi = "kimi";
        var model = "kimi-code/k3";
        var maxContextTokens = 256_000;
        var enableMcp = false;
        for (var i = 0; i < args.Length; i++)
        {
            if (args[i] == "--port" && i + 1 < args.Length && int.TryParse(args[++i], out var value)) port = value;
            else if (args[i] == "--kimi" && i + 1 < args.Length) kimi = args[++i];
            else if (args[i] == "--model" && i + 1 < args.Length) model = args[++i];
            else if (args[i] == "--context-size" && i + 1 < args.Length && int.TryParse(args[++i], out var contextSize) && contextSize > 0) maxContextTokens = contextSize;
            else if (args[i] == "--mcp") enableMcp = true;
        }
        return new FleetOptions(port, kimi, model, maxContextTokens, enableMcp);
    }
}

sealed class FleetHost : IDisposable
{
    private readonly FleetOptions _options;
    private readonly HttpListener _listener = new();
    private readonly ConcurrentDictionary<string, KimiAgent> _agents = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, string> _scopeOwners = new(StringComparer.OrdinalIgnoreCase);
    private readonly object _agentsLock = new();
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
            else if (context.Request.HttpMethod == "GET" && parts.Length == 3 && parts[0] == "agents" && parts[2] == "chat")
                await RespondJsonAsync(context, GetChat(parts[1]));
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
            else if (context.Request.HttpMethod == "POST" && parts.Length == 3 && parts[0] == "agents" && parts[2] == "cancel")
                await CancelAgentAsync(context, parts[1]);
            else if (context.Request.HttpMethod == "POST" && parts.Length == 4 && parts[0] == "agents" && parts[2] == "context" && parts[3] == "compact")
                await CompactContextAsync(context, parts[1]);
            else if (context.Request.HttpMethod == "POST" && parts.Length == 4 && parts[0] == "agents" && parts[2] == "context" && parts[3] == "reset")
                await ResetContextAsync(context, parts[1]);
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

    private async Task CancelAgentAsync(HttpListenerContext context, string name)
    {
        await Cancel(name);
        await RespondJsonAsync(context, new { accepted = true, agent = name, operation = "cancel" });
    }

    private async Task CompactContextAsync(HttpListenerContext context, string name)
    {
        var request = await ReadJsonAsync<CompactContextRequest>(context);
        Compact(name, request.Instructions);
        await RespondJsonAsync(context, new { accepted = true, agent = name, operation = "compact" });
    }

    private async Task ResetContextAsync(HttpListenerContext context, string name)
    {
        await ResetContext(name);
        await RespondJsonAsync(context, new { accepted = true, agent = name, operation = "reset" });
    }

    public async Task<object> StartAgentAsync(AgentSpec spec)
    {
        var name = ValidateName(spec.Name);
        var workspace = Path.GetFullPath(spec.Workspace);
        if (!Directory.Exists(workspace)) throw new InvalidOperationException($"Workspace does not exist: {workspace}");
        var scopes = NormalizeScopes(workspace, spec.Scopes);
        KimiAgent agent;
        lock (_agentsLock)
        {
            if (_agents.ContainsKey(name)) throw new InvalidOperationException($"Agent '{name}' already exists.");
            foreach (var scope in scopes)
            {
                var scopePath = Path.GetFullPath(Path.Combine(workspace, scope));
                var conflict = _scopeOwners.FirstOrDefault(pair => PathsOverlap(scopePath, pair.Key));
                if (!string.IsNullOrEmpty(conflict.Key))
                    throw new InvalidOperationException($"Scope '{scope}' overlaps a scope owned by '{conflict.Value}'.");
            }
            agent = new KimiAgent(name, workspace, spec.Role ?? "worker", scopes, _options, Publish);
            if (!_agents.TryAdd(name, agent)) throw new InvalidOperationException($"Could not add '{name}'.");
            foreach (var scope in scopes) _scopeOwners[Path.GetFullPath(Path.Combine(workspace, scope))] = name;
        }
        try { await agent.StartAsync(); return agent.Snapshot(); }
        catch { RemoveAgent(name); throw; }
    }

    public IReadOnlyList<object> ListAgents() => _agents.Values.Select(a => a.Snapshot()).Cast<object>().ToArray();
    public IReadOnlyList<object> GetChat(string name) => GetAgent(name).ChatSnapshot();
    public void Assign(string name, string prompt) => _ = GetAgent(name).PromptAsync(prompt);
    public void Compact(string name, string? instructions) => _ = GetAgent(name).CompactContextAsync(instructions);
    public Task Cancel(string name) => GetAgent(name).CancelAsync();
    public Task ResetContext(string name) => GetAgent(name).ResetContextAsync();
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
        KimiAgent? agent;
        lock (_agentsLock)
        {
            if (!_agents.TryRemove(name, out agent)) return;
            foreach (var scope in agent.Scopes)
                _scopeOwners.TryRemove(Path.GetFullPath(Path.Combine(agent.Workspace, scope)), out _);
        }
        agent.Dispose();
        Publish(name, "stopped", "Agent stopped.");
    }

    private static string[] NormalizeScopes(string workspace, string[]? requestedScopes)
    {
        if (requestedScopes is null) return [];
        var scopes = new List<string>();
        foreach (var requested in requestedScopes)
        {
            if (string.IsNullOrWhiteSpace(requested)) continue;
            var fullPath = Path.GetFullPath(Path.Combine(workspace, requested.Trim()));
            if (!IsWithin(fullPath, workspace)) throw new InvalidOperationException($"Scope escaped the workspace: {requested}");
            var relative = Path.GetRelativePath(workspace, fullPath);
            if (!scopes.Contains(relative, StringComparer.OrdinalIgnoreCase)) scopes.Add(relative);
        }
        return scopes.ToArray();
    }

    private static bool PathsOverlap(string left, string right) => IsWithin(left, right) || IsWithin(right, left);

    private static bool IsWithin(string path, string directory)
    {
        var relative = Path.GetRelativePath(directory, path);
        return relative != ".." && !relative.StartsWith($"..{Path.DirectorySeparatorChar}", StringComparison.Ordinal);
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
        var channel = Channel.CreateBounded<string>(new BoundedChannelOptions(256)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = true,
            SingleWriter = false
        });
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
            if (type == "tool_call_update")
            {
                var title = update.TryGetProperty("title", out var titleNode) ? titleNode.GetString() : null;
                var status = update.TryGetProperty("status", out var statusNode) ? statusNode.GetString() : null;
                return $"{title ?? "Tool call"} — {status ?? "updated"}";
            }
            if (type == "agent_message_chunk") return "Kimi sent a response";
            return type ?? "session update";
        }
        catch (Exception) { return "session update"; }
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
    private sealed record CompactContextRequest(string? Instructions);
}

sealed class KimiAgent : IDisposable
{
    private readonly FleetOptions _options;
    private readonly Action<string, string, string> _publish;
    private readonly Process _process;
    private readonly ConcurrentDictionary<long, TaskCompletionSource<JsonElement>> _pending = new();
    private readonly SemaphoreSlim _stdinLock = new(1, 1);
    private readonly SemaphoreSlim _promptGate = new(1, 1);
    private readonly List<AgentChatEntry> _chat = [];
    private readonly Dictionary<string, AgentChatEntry> _toolEntries = [];
    private readonly object _chatLock = new();
    private readonly object _promptLifecycleLock = new();
    private long _requestId;
    private long _chatId;
    private long _estimatedTokens;
    private int _queuedPrompts;
    private int _cancellationRequested;
    private string? _sessionId;
    private string? _assistantEntryId;
    private string? _thoughtEntryId;
    private bool _resettingContext;
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
        _process.Exited += (_, _) => HandleProcessExit();
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
        AddChat("system", $"Kimi K3 session ready · {Role}");
        _publish(Name, "ready", $"Role={Role}; scopes={string.Join(", ", Scopes)}");
    }

    public Task PromptAsync(string prompt) => QueuePromptAsync(prompt);

    private Task<bool> QueuePromptAsync(string prompt)
    {
        if (string.IsNullOrWhiteSpace(prompt)) throw new InvalidOperationException("Prompt must not be empty.");
        if (_sessionId is null) throw new InvalidOperationException($"Agent '{Name}' has no ACP session.");
        int queuedCount;
        lock (_promptLifecycleLock)
        {
            if (_resettingContext) throw new InvalidOperationException("Cannot queue a prompt while context is being reset.");
            AddChat("user", prompt);
            AddEstimatedTokens(prompt);
            queuedCount = Interlocked.Increment(ref _queuedPrompts);
        }
        return RunPromptAsync(prompt, queuedCount);
    }

    private async Task<bool> RunPromptAsync(string prompt, int queuedCount)
    {
        if (_promptGate.CurrentCount == 0) _publish(Name, "queued", $"Task queued at position {queuedCount}.");
        await _promptGate.WaitAsync();
        Interlocked.Decrement(ref _queuedPrompts);
        try
        {
            State = "working";
            SetActivity("Planning assigned task");
            _publish(Name, "prompt", prompt.Length > 160 ? prompt[..160] + "…" : prompt);
            await RequestAsync("session/prompt", new { sessionId = _sessionId, prompt = new[] { new { type = "text", text = prompt } } });
            State = "ready";
            SetActivity("Ready for a task");
            var cancelled = Interlocked.Exchange(ref _cancellationRequested, 0) == 1;
            _publish(Name, cancelled ? "cancelled" : "completed", cancelled ? "Task cancelled." : "Prompt completed.");
            return true;
        }
        catch (Exception ex)
        {
            Interlocked.Exchange(ref _cancellationRequested, 0);
            State = "error";
            SetActivity("Needs attention");
            AddChat("error", ex.Message);
            _publish(Name, "error", ex.Message);
            return false;
        }
        finally { _promptGate.Release(); }
    }

    public async Task CompactContextAsync(string? instructions)
    {
        var command = string.IsNullOrWhiteSpace(instructions) ? "/compact" : $"/compact {instructions.Trim()}";
        if (await QueuePromptAsync(command))
        {
            Interlocked.Exchange(ref _estimatedTokens, Math.Min(Interlocked.Read(ref _estimatedTokens), _options.MaxContextTokens / 4));
            AddChat("system", "Context compacted");
            _publish(Name, "context", "Context compacted.");
        }
    }

    public async Task ResetContextAsync()
    {
        if (!await _promptGate.WaitAsync(0)) throw new InvalidOperationException("Cannot reset context while the agent is working.");
        try
        {
            lock (_promptLifecycleLock)
            {
                if (Volatile.Read(ref _queuedPrompts) > 0) throw new InvalidOperationException("Cannot reset context while prompts are queued.");
                _resettingContext = true;
            }
            try
            {
                var session = await RequestAsync("session/new", new { cwd = Workspace, mcpServers = Array.Empty<object>() });
                _sessionId = session.GetProperty("sessionId").GetString() ?? throw new InvalidOperationException("ACP did not return sessionId.");
                lock (_chatLock)
                {
                    _chat.Clear();
                    _toolEntries.Clear();
                    _assistantEntryId = null;
                    _thoughtEntryId = null;
                }
                Interlocked.Exchange(ref _estimatedTokens, 0);
                State = "ready";
                SetActivity("Ready for a task");
                AddChat("system", "Fresh Kimi K3 context created");
                _publish(Name, "context", "Context reset.");
            }
            finally { lock (_promptLifecycleLock) _resettingContext = false; }
        }
        finally { _promptGate.Release(); }
    }

    public async Task CancelAsync()
    {
        if (_sessionId is null) throw new InvalidOperationException($"Agent '{Name}' has no ACP session.");
        if (State != "working") throw new InvalidOperationException($"Agent '{Name}' is not working.");
        await SendRawAsync(JsonSerializer.Serialize(new { jsonrpc = "2.0", method = "session/cancel", @params = new { sessionId = _sessionId } }));
        Interlocked.Exchange(ref _cancellationRequested, 1);
        SetActivity("Cancelling current task");
        AddChat("system", "Cancellation requested");
        _publish(Name, "cancel", "Cancellation requested.");
    }

    private async Task<JsonElement> RequestAsync(string method, object parameters)
    {
        var id = Interlocked.Increment(ref _requestId);
        var completion = new TaskCompletionSource<JsonElement>(TaskCreationOptions.RunContinuationsAsynchronously);
        if (!_pending.TryAdd(id, completion)) throw new InvalidOperationException("Could not register ACP request.");
        var envelope = JsonSerializer.Serialize(new { jsonrpc = "2.0", id, method, @params = parameters });
        try
        {
            await SendRawAsync(envelope);
            var requestTimeout = method == "session/prompt" ? TimeSpan.FromHours(2) : TimeSpan.FromMinutes(2);
            using var timeout = new CancellationTokenSource(requestTimeout);
            await using var registration = timeout.Token.Register(() => completion.TrySetException(new TimeoutException($"ACP request '{method}' timed out.")));
            return await completion.Task;
        }
        finally { _pending.TryRemove(id, out _); }
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
            if (root.TryGetProperty("method", out var method))
            {
                var methodName = method.GetString() ?? "notification";
                if (root.TryGetProperty("id", out var requestId))
                {
                    var parameters = root.TryGetProperty("params", out var requestParams) ? requestParams.Clone() : default;
                    _ = HandleClientRequestAsync(methodName, requestId.GetRawText(), parameters);
                    return;
                }
                if (methodName == "session/update" && root.TryGetProperty("params", out var updateParams))
                {
                    UpdateActivityFromSessionUpdate(updateParams);
                    UpdateChatFromSessionUpdate(updateParams);
                }
                _publish(Name, methodName, root.TryGetProperty("params", out var p) ? p.GetRawText() : string.Empty);
                return;
            }
            if (root.TryGetProperty("id", out var idNode) && idNode.TryGetInt64(out var id) && _pending.TryRemove(id, out var completion))
            {
                if (root.TryGetProperty("error", out var error)) completion.TrySetException(new InvalidOperationException(error.GetRawText()));
                else if (root.TryGetProperty("result", out var result)) completion.TrySetResult(result.Clone());
                else completion.TrySetException(new InvalidOperationException("ACP response has neither result nor error."));
                return;
            }
            _publish(Name, "protocol", line);
        }
        catch (Exception ex)
        {
            _publish(Name, "protocol-error", ex.Message);
            _publish(Name, "stdout", line);
        }
    }

    private async Task HandleClientRequestAsync(string method, string requestId, JsonElement parameters)
    {
        try
        {
            object result;
            switch (method)
            {
                case "session/request_permission":
                    var optionId = SelectApproval(parameters);
                    SetActivity("Approving requested action");
                    AddChat("system", "Permission approved automatically");
                    _publish(Name, "permission", $"Auto-approved '{optionId}' (yolo mode).");
                    result = new { outcome = new { outcome = "selected", optionId } };
                    break;
                case "fs/read_text_file":
                    var readPath = ResolveWorkspacePath(parameters);
                    SetActivity($"Reading {Path.GetFileName(readPath)}");
                    _publish(Name, "file", $"Reading {Path.GetRelativePath(Workspace, readPath)}");
                    var fileContent = await File.ReadAllTextAsync(readPath);
                    AddEstimatedTokens(fileContent);
                    result = new { content = fileContent };
                    break;
                case "fs/write_text_file":
                    var writePath = ResolveWorkspacePath(parameters);
                    EnsureWritableScope(writePath);
                    if (!parameters.TryGetProperty("content", out var contentNode) || contentNode.ValueKind != JsonValueKind.String)
                        throw new InvalidOperationException("ACP write request has no text content.");
                    Directory.CreateDirectory(Path.GetDirectoryName(writePath)!);
                    await File.WriteAllTextAsync(writePath, contentNode.GetString()!);
                    AddEstimatedTokens(contentNode.GetString()!);
                    SetActivity($"Editing {Path.GetFileName(writePath)}");
                    _publish(Name, "file", $"Updated {Path.GetRelativePath(Workspace, writePath)}");
                    result = new { };
                    break;
                default:
                    throw new InvalidOperationException($"Unsupported ACP client request '{method}'.");
            }
            await SendResultAsync(requestId, result);
        }
        catch (Exception ex)
        {
            _publish(Name, "client-error", $"{method}: {ex.Message}");
            await SendErrorAsync(requestId, ex.Message);
        }
    }

    private string ResolveWorkspacePath(JsonElement parameters)
    {
        if (!parameters.TryGetProperty("path", out var pathNode) || pathNode.ValueKind != JsonValueKind.String || string.IsNullOrWhiteSpace(pathNode.GetString()))
            throw new InvalidOperationException("ACP filesystem request has no path.");
        var requested = pathNode.GetString()!;
        var fullPath = Path.GetFullPath(Path.IsPathRooted(requested) ? requested : Path.Combine(Workspace, requested));
        if (!IsWithin(fullPath, Workspace)) throw new UnauthorizedAccessException("ACP filesystem request escaped the agent workspace.");
        return fullPath;
    }

    private void EnsureWritableScope(string path)
    {
        if (Scopes.Count == 0) return;
        if (Scopes.Any(scope => IsWithin(path, Path.GetFullPath(Path.Combine(Workspace, scope))))) return;
        throw new UnauthorizedAccessException("ACP write request is outside the agent's exclusive scopes.");
    }

    private static bool IsWithin(string path, string directory)
    {
        var relative = Path.GetRelativePath(directory, path);
        return relative != ".." && !relative.StartsWith($"..{Path.DirectorySeparatorChar}", StringComparison.Ordinal);
    }

    private void UpdateActivityFromSessionUpdate(JsonElement parameters)
    {
        if (!parameters.TryGetProperty("update", out var update) || !update.TryGetProperty("sessionUpdate", out var typeNode)) return;
        var type = typeNode.GetString();
        if ((type == "tool_call" || type == "tool_call_update") && update.TryGetProperty("title", out var title) && !string.IsNullOrWhiteSpace(title.GetString()))
        {
            var status = update.TryGetProperty("status", out var statusNode) ? statusNode.GetString() : null;
            SetActivity(status is "completed" or "failed" ? "Reviewing tool result" : title.GetString()!);
        }
        else if (type == "agent_thought_chunk")
            SetActivity("Analyzing task");
        else if (type == "agent_message_chunk")
            SetActivity("Preparing a report");
    }

    private void UpdateChatFromSessionUpdate(JsonElement parameters)
    {
        if (!parameters.TryGetProperty("update", out var update) || !update.TryGetProperty("sessionUpdate", out var typeNode)) return;
        var type = typeNode.GetString();
        if (type == "agent_message_chunk")
        {
            var text = ExtractText(update.TryGetProperty("content", out var content) ? content : default);
            AppendChatChunk("assistant", text, ref _assistantEntryId);
            AddEstimatedTokens(text);
        }
        else if (type == "agent_thought_chunk")
        {
            var text = ExtractText(update.TryGetProperty("content", out var content) ? content : default);
            AppendChatChunk("thought", text, ref _thoughtEntryId);
            AddEstimatedTokens(text);
        }
        else if (type is "tool_call" or "tool_call_update")
        {
            UpsertToolEntry(update);
            _assistantEntryId = null;
            _thoughtEntryId = null;
        }
    }

    private void UpsertToolEntry(JsonElement update)
    {
        if (!update.TryGetProperty("toolCallId", out var idNode) || string.IsNullOrWhiteSpace(idNode.GetString())) return;
        var toolCallId = idNode.GetString()!;
        lock (_chatLock)
        {
            if (!_toolEntries.TryGetValue(toolCallId, out var entry))
            {
                entry = NewChatEntry("tool", string.Empty);
                entry.ToolCallId = toolCallId;
                _toolEntries[toolCallId] = entry;
                AddChatEntryLocked(entry);
            }
            if (update.TryGetProperty("title", out var title) && !string.IsNullOrWhiteSpace(title.GetString())) entry.Title = title.GetString();
            if (update.TryGetProperty("status", out var status) && !string.IsNullOrWhiteSpace(status.GetString())) entry.Status = status.GetString();
            if (update.TryGetProperty("kind", out var kind) && !string.IsNullOrWhiteSpace(kind.GetString())) entry.ToolKind = kind.GetString();
            if (update.TryGetProperty("content", out var content))
            {
                var text = ExtractText(content);
                if (!string.IsNullOrWhiteSpace(text)) entry.Text = LimitText(text, 12_000);
            }
        }
    }

    private void AppendChatChunk(string type, string text, ref string? activeEntryId)
    {
        if (string.IsNullOrEmpty(text)) return;
        lock (_chatLock)
        {
            AgentChatEntry? entry = null;
            if (activeEntryId is not null)
            {
                for (var i = _chat.Count - 1; i >= 0; i--)
                    if (_chat[i].Id == activeEntryId) { entry = _chat[i]; break; }
            }
            if (entry is null)
            {
                entry = NewChatEntry(type, string.Empty);
                AddChatEntryLocked(entry);
                activeEntryId = entry.Id;
            }
            entry.Text = LimitText(entry.Text + text, 48_000);
        }
    }

    private void AddChat(string type, string text)
    {
        lock (_chatLock)
        {
            AddChatEntryLocked(NewChatEntry(type, LimitText(text, 48_000)));
            if (type == "user")
            {
                _assistantEntryId = null;
                _thoughtEntryId = null;
            }
        }
    }

    private AgentChatEntry NewChatEntry(string type, string text) => new()
    {
        Id = $"{Name}-{Interlocked.Increment(ref _chatId)}",
        Time = DateTimeOffset.UtcNow,
        Type = type,
        Text = text
    };

    private void AddChatEntryLocked(AgentChatEntry entry)
    {
        _chat.Add(entry);
        while (_chat.Count > 500)
        {
            var removed = _chat[0];
            _chat.RemoveAt(0);
            if (removed.ToolCallId is not null) _toolEntries.Remove(removed.ToolCallId);
        }
    }

    private static string ExtractText(JsonElement value)
    {
        if (value.ValueKind == JsonValueKind.Undefined || value.ValueKind == JsonValueKind.Null) return string.Empty;
        if (value.ValueKind == JsonValueKind.String) return value.GetString() ?? string.Empty;
        if (value.ValueKind == JsonValueKind.Array) return string.Concat(value.EnumerateArray().Select(ExtractText));
        if (value.ValueKind != JsonValueKind.Object) return string.Empty;
        if (value.TryGetProperty("text", out var text) && text.ValueKind == JsonValueKind.String) return text.GetString() ?? string.Empty;
        if (value.TryGetProperty("content", out var content)) return ExtractText(content);
        return string.Empty;
    }

    private void AddEstimatedTokens(string text)
    {
        if (!string.IsNullOrEmpty(text)) Interlocked.Add(ref _estimatedTokens, Math.Max(1, text.Length / 4));
    }

    private static string LimitText(string text, int maxLength) => text.Length <= maxLength ? text : $"…{text[^maxLength..]}";

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

    private Task SendResultAsync(string requestId, object result) =>
        SendRawAsync($"{{\"jsonrpc\":\"2.0\",\"id\":{requestId},\"result\":{JsonSerializer.Serialize(result)}}}");

    private Task SendErrorAsync(string requestId, string message) =>
        SendRawAsync($"{{\"jsonrpc\":\"2.0\",\"id\":{requestId},\"error\":{{\"code\":-32000,\"message\":{JsonSerializer.Serialize(message)}}}}}");

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

    public IReadOnlyList<object> ChatSnapshot()
    {
        lock (_chatLock) return _chat.Select(entry => entry.Snapshot()).ToArray();
    }

    public object Snapshot()
    {
        var estimatedTokens = Interlocked.Read(ref _estimatedTokens);
        return new
        {
            name = Name,
            workspace = Workspace,
            role = Role,
            scopes = Scopes,
            state = State,
            activity = Activity,
            activityUpdatedAt = ActivityUpdatedAt,
            queueDepth = Volatile.Read(ref _queuedPrompts),
            contextTokens = estimatedTokens,
            maxContextTokens = _options.MaxContextTokens,
            contextPercent = Math.Round(Math.Min(100d, estimatedTokens * 100d / _options.MaxContextTokens), 1),
            sessionId = _sessionId
        };
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        FailPending(new ObjectDisposedException(Name, "Agent stopped."));
        try
        {
            if (!_process.HasExited) _process.Kill(entireProcessTree: true);
            _process.WaitForExit(2_000);
        }
        catch (InvalidOperationException) { }
        _process.Dispose();
    }

    private void HandleProcessExit()
    {
        State = "stopped";
        SetActivity("Kimi process stopped");
        var exitCode = 0;
        try { exitCode = _process.ExitCode; } catch (Exception) when (_disposed) { }
        var error = new InvalidOperationException($"Kimi process exited with code {exitCode}.");
        FailPending(error);
        _publish(Name, "exit", error.Message);
    }

    private void FailPending(Exception error)
    {
        foreach (var request in _pending.ToArray())
            if (_pending.TryRemove(request.Key, out var completion)) completion.TrySetException(error);
    }
}
