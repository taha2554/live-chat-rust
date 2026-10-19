# live-chat-rust

![CI](https://github.com/taha2554/live-chat-rust/actions/workflows/ci.yml/badge.svg)

Real-time chat rooms over WebSockets, built with **Rust**, **Axum** and **Tokio**. The server and a small vanilla-JS client ship as one binary.

## How it works

- Each room has a `tokio::sync::broadcast` channel. Every connection subscribes to its room's channel and forwards events to its socket.
- A connection is split into a **receive task** (reads messages from the client, validates them, publishes to the room) and a **send task** (writes room events + private error messages to the client). When either finishes, the other is aborted and the user is removed from the room.
- Rooms keep the last 50 messages so people who join later see recent history. When the last user leaves, the room is dropped.
- If a client is too slow and the broadcast buffer overflows, it skips the missed events (`RecvError::Lagged`) instead of being disconnected.

## Features

- Multiple rooms, user list per room, join/leave notices
- Duplicate names in the same room get a suffix (`taha`, `taha2`, …)
- Message validation (trimmed, max 500 characters) and per-connection flood limit (5 messages / 5 s)
- `GET /api/rooms` – active rooms sorted by number of people online
- `GET /health` for container health checks
- Responsive client with dark mode, no build step (`static/index.html` is embedded with `include_str!`)

## Protocol

Connect to `ws://host/ws?room=<room>&name=<name>` and send:

```json
{ "text": "hello" }
```

The server sends JSON events with a `type` field:

```json
{ "type": "history", "messages": [ ... ] }
{ "type": "message", "user": "taha", "text": "hello", "ts": 1760880000000 }
{ "type": "join",    "user": "ayse", "online": ["taha", "ayse"] }
{ "type": "leave",   "user": "ayse", "online": ["taha"] }
{ "type": "error",   "message": "you're sending messages too fast" }
```

## Running

```bash
cargo run
# open http://localhost:3000 in two browser windows
```

Environment variables: `PORT` (default 3000), `RUST_LOG` (default `live_chat_rust=info`).

```bash
docker build -t live-chat .
docker run -p 3000:3000 live-chat
```

## Tests

```bash
cargo test
```

Unit tests cover the room state (name suffixes, history limit, room cleanup). The integration tests in `tests/ws.rs` start the real server on a random port and connect several WebSocket clients to check broadcasting, room isolation, history, leave events, validation errors and the flood limit.

## License

MIT
