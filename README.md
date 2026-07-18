# KimiFleet

Visible multi-agent orchestrator and MCP server for Kimi Code K3. It controls Kimi through ACP (JSON-RPC over stdio), rather than attempting to drive its terminal UI.

## Start

```bash
dotnet run --project KimiFleet.csproj -- --port 7373
```

Open a second terminal and watch live activity:

```bash
curl -N http://127.0.0.1:7373/events
```

Open `http://127.0.0.1:7373` for the cockpit. Every agent has its own Kimi-style chat, tool-call transcript, task composer, live activity, queue depth, scope ownership and estimated K3 context usage. The grid supports search, compact density and a single-agent focus mode.

Context controls are available on every card:

- **Compact** asks Kimi to preserve decisions, constraints, changed files and pending verification while reducing context pressure.
- **Fresh context** creates a new ACP session without changing files, the agent process or its scope locks.
- **Cancel** interrupts the current turn without stopping the instance or discarding its context.
- New prompts are queued per agent, so one instance cannot receive overlapping `session/prompt` calls.

## MCP mode

Start both the dashboard and an MCP stdio server:

```bash
dotnet run --project KimiFleet.csproj -- --mcp --port 7373
```

The server implements `fleet_list_agents`, `fleet_start_agent`, `fleet_get_chat`, `fleet_assign_task`, `fleet_cancel_task`, `fleet_compact_context`, `fleet_reset_context`, `fleet_request_review`, and `fleet_stop_agent`. In MCP mode stdout is protocol-only; operational logs are written to stderr and remain visible in the dashboard.

## API

Create an agent with exclusive source scopes:

```bash
curl -X POST http://127.0.0.1:7373/agents \
  -H 'Content-Type: application/json' \
  -d '{"name":"vulkan","workspace":"/home/emil/Desktop/Cortex_Engine","role":"renderer","scopes":["src/Engine.Graphics.Vulkan","src/Engine.Graphics"]}'
```

Give it work:

```bash
curl -X POST http://127.0.0.1:7373/agents/vulkan/prompt \
  -H 'Content-Type: application/json' \
  -d '{"prompt":"Read AGENTS.md, inspect the Vulkan backend, and propose one safe improvement."}'
```

Ask another agent to review the current diff without editing:

```bash
curl -X POST http://127.0.0.1:7373/agents/reviewer/review \
  -H 'Content-Type: application/json' \
  -d '{"author":"vulkan","context":"Review the Vulkan change after its tests finish."}'
```

Useful endpoints:

- `GET /health`, `GET /agents`, `GET /agents/{name}/chat`, `GET /events`
- `POST /agents`, `POST /agents/{name}/prompt`, `POST /agents/{name}/cancel`, `POST /agents/{name}/review`, `POST /agents/{name}/stop`
- `POST /agents/{name}/context/compact`, `POST /agents/{name}/context/reset`

All ACP children launch as `kimi --yolo --model kimi-code/k3 acp`. Scope locks reject exact and nested overlaps in the same workspace. They coordinate ownership; yolo agents can still run shell commands, so the locks do not replace peer review or a final `git status` check before edits.
