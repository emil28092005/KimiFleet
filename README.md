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

The server implements `fleet_list_agents`, `fleet_start_agent`, `fleet_get_chat`, `fleet_assign_task`, `fleet_cancel_task`, `fleet_compact_context`, `fleet_reset_context`, `fleet_request_review`, `fleet_handoff_context`, `fleet_list_handoffs`, and `fleet_stop_agent`. In MCP mode stdout is protocol-only; operational logs are written to stderr and remain visible in the dashboard.

`fleet_handoff_context` requires `from`, `recipients` and `kind`, and accepts optional `title`, `instructions` and `message`. With `kind: "message"` the literal `message` text is delivered unchanged; with `context`, `delegate`, `review` or `broadcast` the source Kimi generates a structured context package from its current session plus `title`/`instructions`, and KimiFleet injects it into the recipients' model contexts. `fleet_list_handoffs` takes an optional `agent` and returns the recorded handoff history.

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
- `POST /agents/{from}/handoff`, `GET /handoffs?agent={name}`

All ACP children launch as `kimi --yolo --model kimi-code/k3 acp`. Scope locks reject exact and nested overlaps in the same workspace. They coordinate ownership; yolo agents can still run shell commands, so the locks do not replace peer review or a final `git status` check before edits.

## Agent-to-agent context bus

Agents hand context to each other through fleet handoffs instead of pasting transcripts into prompts. Every new or reset Kimi session is silently given its own fleet URL and REST handoff contract, so an agent can initiate a transfer itself with its Bash tool; the dashboard and an external MCP orchestrator can initiate the same operation on its behalf. A handoff names a sender (`from`), one or more `recipients`, and a `kind`:

- `message` — direct handoff. The trimmed `message` text is delivered without asking the source to generate a summary.
- `context`, `delegate`, `review`, `broadcast` — generated handoffs. The source Kimi summarizes its live session into a structured package, guided by `title` and `instructions` (plus any extra `message` detail): `context` shares working context, `delegate` assigns a bounded piece of work, `review` asks for read-only peer review, and `broadcast` delivers the same package to every recipient.

Generated packages are injected into each recipient's model context, so they consume the recipients' token budget — keep titles and instructions focused. The same applies on the MCP side via `fleet_handoff_context`.

Direct message between two agents:

```bash
curl -X POST http://127.0.0.1:7373/agents/vulkan/handoff \
  -H 'Content-Type: application/json' \
  -d '{"recipients":["reviewer"],"kind":"message","title":"Render-pass refactor ready","message":"The refactor is done and tests pass; please take a look when free."}'
```

Generated delegate package:

```bash
curl -X POST http://127.0.0.1:7373/agents/lead/handoff \
  -H 'Content-Type: application/json' \
  -d '{"recipients":["vulkan"],"kind":"delegate","title":"Validation layer cleanup","instructions":"Enable Vulkan validation layers in debug builds only and document the toggle."}'
```

Generated review package:

```bash
curl -X POST http://127.0.0.1:7373/agents/vulkan/handoff \
  -H 'Content-Type: application/json' \
  -d '{"recipients":["reviewer"],"kind":"review","title":"Review render-pass refactor","instructions":"Review only, do not edit. Check synchronization and resource lifetime in the current diff."}'
```

Generated broadcast to several agents:

```bash
curl -X POST http://127.0.0.1:7373/agents/lead/handoff \
  -H 'Content-Type: application/json' \
  -d '{"recipients":["vulkan","reviewer","docs"],"kind":"broadcast","title":"Swapchain API freeze","instructions":"The swapchain API is frozen until Friday; base all pending work on it."}'
```

Query handoff history, filtered to one agent or across the fleet:

```bash
curl 'http://127.0.0.1:7373/handoffs?agent=vulkan'
curl http://127.0.0.1:7373/handoffs
```

Delivery and history semantics:

- `POST /agents/{from}/handoff` validates the sender and recipients, records the handoff, and returns the accepted record (id, timestamp, sender, recipients, kind, title).
- Delivery is asynchronous: a handoff for a busy agent is queued behind its current task and delivered in order, using the same per-agent prompt queue as regular tasks, so one instance never receives overlapping prompts.
- The host retains handoff history for the lifetime of the process; `GET /handoffs` returns the full history, with the optional `agent` filter keeping only handoffs sent by or addressed to that agent.
- In the dashboard, handoffs surface in the receiving agent's chat transcript and in the live event feed (`/events`), so you can watch context move between agents as it happens.
