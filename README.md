# Go Relay Agent Daemon

A high-performance relay daemon for the XMRT DAO agent meshnet, replacing the existing TypeScript relay.

## Architecture

```
┌─────────────┐     ┌──────────────────┐     ┌──────────────┐
│  Supabase   │────▶│  Go Relay Daemon │◀────│  Agents (WS) │
│  Webhooks   │     │    (port 8081)   │     │  Eliza-Dev   │
└─────────────┘     │                  │     │  Hermes      │
                    │  ┌────────────┐  │     │  Alice       │
                    │  │ Task Queue │  │     └──────────────┘
                    │  │ (priority) │  │
                    │  └────────────┘  │
                    │                  │
                    │  ┌────────────┐  │
                    │  │  REST API  │  │
                    │  │ /health    │  │
                    │  │ /status    │  │
                    │  │ /agents    │  │
                    │  │ /tasks     │  │
                    │  └────────────┘  │
                    └──────────────────┘
```

## Features

- **WebSocket agent connections** — agents connect via WS, receive heartbeats, get task dispatch
- **Priority task queue** — tasks are dispatched by priority (heap-based)
- **REST API** — full CRUD for agents and tasks
- **Supabase webhook receiver** — receives tasks from Supabase edge functions
- **Graceful shutdown** — SIGTERM → drain connections → exit
- **Agent lifecycle** — auto-register, heartbeat monitoring, stale agent reaping
- **CORS enabled** — all origins allowed for development

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Basic health check |
| GET | `/status` | Full system status |
| GET | `/api/v1/agents` | List all agents |
| GET | `/api/v1/agents/{id}` | Get agent by ID |
| DELETE | `/api/v1/agents/{id}` | Unregister agent |
| GET | `/api/v1/tasks` | List tasks (filter: ?status=pending) |
| POST | `/api/v1/tasks` | Create a task |
| GET | `/api/v1/tasks/{id}` | Get task by ID |
| PATCH | `/api/v1/tasks/{id}` | Update task status |
| WS | `/ws` | Agent WebSocket connection |
| POST | `/webhook/task` | Supabase task webhook |

## Quick Start

```bash
# Build
go build -o relayd ./cmd/relayd/

# Run with defaults
./relayd

# Run with custom port
./relayd --port 9090

# Run with config file
./relayd --config /path/to/config.json
```

## Configuration

Config via `config.json` or environment variables:

| Env Var | Config Key | Default |
|---------|-----------|---------|
| `RELAY_HOST` | `host` | `0.0.0.0` |
| `RELAY_PORT` | `port` | `8081` |
| `SUPABASE_URL` | `supabase_url` | `""` |
| `SUPABASE_API_KEY` | `supabase_api_key` | `""` |
| `RELAY_LOG_LEVEL` | `log_level` | `info` |

## Agent WebSocket Protocol

1. Agent connects to `ws://host:port/ws`
2. Agent sends registration:
   ```json
   {"type":"register","payload":{"id":"hermes","name":"Hermes","role":"phone-agent","capabilities":["bash","python","node"],"endpoint":"http://..."}}
   ```
3. Server acknowledges: `{"type":"registered","payload":{"status":"ok"}}`
4. Heartbeat ping/pong every 15s
5. Agent sends status updates: `{"type":"status_update","payload":{"status":"busy"}}`
6. Server dispatches tasks via WebSocket messages

## Build for Different Targets

```bash
# Linux
GOOS=linux GOARCH=amd64 go build -o relayd-linux ./cmd/relayd/

# ARM64 (phone/Hermes)
GOOS=linux GOARCH=arm64 go build -o relayd-arm64 ./cmd/relayd/

# Windows
GOOS=windows GOARCH=amd64 go build -o relayd.exe ./cmd/relayd/
```

## Next Steps

- [ ] Agent dispatch: push tasks to connected agents via WebSocket
- [ ] Supabase sync: poll/push task status back to Supabase
- [ ] TLS support
- [ ] Metrics endpoint (Prometheus)
- [ ] CLI tool for fleet management (`fleet status`, `fleet dispatch`)
- [ ] Dockerfile + container deployment
