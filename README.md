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

Open `http://127.0.0.1:7373` for the dashboard: it shows agents, their scoped ownership, state and a human-readable live activity feed.

## MCP mode

Start both the dashboard and an MCP stdio server:

```bash
dotnet run --project KimiFleet.csproj -- --mcp --port 7373
```

The server implements `fleet_list_agents`, `fleet_start_agent`, `fleet_assign_task`, `fleet_request_review`, and `fleet_stop_agent`. In MCP mode stdout is protocol-only; operational logs are written to stderr and remain visible in the dashboard.

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

Useful endpoints: `GET /health`, `GET /agents`, `GET /events`, `POST /agents/{name}/stop`.

All ACP children launch as `kimi --yolo --model kimi-code/k3 acp`. Scope locks prevent two fleet agents from being assigned the same area, but they do not replace a final `git status` check before edits.
