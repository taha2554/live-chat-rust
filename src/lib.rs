pub mod state;

use std::time::{Duration, Instant};

use axum::{
    Json, Router,
    extract::{
        Query, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    routing::get,
};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::sync::broadcast::error::RecvError;

pub use state::{AppState, Event};
use state::{clean_message, valid_name};

const INDEX_HTML: &str = include_str!("../static/index.html");

/// Allow at most this many messages per window per connection.
const FLOOD_LIMIT: usize = 5;
const FLOOD_WINDOW: Duration = Duration::from_secs(5);

pub fn app(state: AppState) -> Router {
    Router::new()
        .route("/", get(|| async { Html(INDEX_HTML) }))
        .route("/health", get(|| async { "ok" }))
        .route("/api/rooms", get(list_rooms))
        .route("/ws", get(ws_handler))
        .with_state(state)
}

async fn list_rooms(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.list().await)
}

#[derive(Deserialize)]
struct JoinParams {
    room: String,
    name: String,
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    Query(params): Query<JoinParams>,
    State(state): State<AppState>,
) -> Response {
    let room = params.room.trim().to_lowercase();
    let name = params.name.trim().to_string();
    if !valid_name(&room) || !valid_name(&name) {
        return (
            StatusCode::BAD_REQUEST,
            "room and name must be 1-24 letters, digits, - or _",
        )
            .into_response();
    }
    ws.on_upgrade(move |socket| handle_socket(socket, state, room, name))
}

#[derive(Deserialize)]
struct Incoming {
    text: String,
}

async fn handle_socket(socket: WebSocket, state: AppState, room: String, wanted: String) {
    let (name, mut rx, history) = state.join(&room, &wanted).await;
    tracing::info!(%room, user = %name, "joined");

    let (mut sink, mut stream) = socket.split();

    // History first, so the client can render the backlog before live events.
    let first = serde_json::to_string(&Event::History { messages: history }).unwrap();
    if sink.send(Message::Text(first.into())).await.is_err() {
        state.leave(&room, &name).await;
        return;
    }

    // Errors meant only for this client go through a small mpsc channel into the send task.
    let (direct_tx, mut direct_rx) = tokio::sync::mpsc::channel::<Event>(8);

    let mut send_task = tokio::spawn(async move {
        loop {
            let event = tokio::select! {
                msg = rx.recv() => match msg {
                    Ok(event) => event,
                    // slow client: skip what it missed instead of disconnecting
                    Err(RecvError::Lagged(_)) => continue,
                    Err(RecvError::Closed) => break,
                },
                Some(event) = direct_rx.recv() => event,
            };
            let json = serde_json::to_string(&event).unwrap();
            if sink.send(Message::Text(json.into())).await.is_err() {
                break;
            }
        }
    });

    let recv_state = state.clone();
    let (recv_room, recv_name) = (room.clone(), name.clone());
    let mut recv_task = tokio::spawn(async move {
        let mut recent: Vec<Instant> = Vec::new();
        while let Some(Ok(msg)) = stream.next().await {
            let text = match msg {
                Message::Text(t) => t,
                Message::Close(_) => break,
                _ => continue,
            };

            let error = match serde_json::from_str::<Incoming>(&text) {
                Err(_) => Some("expected {\"text\": \"...\"}".to_string()),
                Ok(incoming) => match clean_message(&incoming.text) {
                    Err(e) => Some(e.to_string()),
                    Ok(clean) => {
                        let now = Instant::now();
                        recent.retain(|t| now.duration_since(*t) < FLOOD_WINDOW);
                        if recent.len() >= FLOOD_LIMIT {
                            Some("you're sending messages too fast".to_string())
                        } else {
                            recent.push(now);
                            recv_state.say(&recv_room, &recv_name, &clean).await;
                            None
                        }
                    }
                },
            };
            if let Some(message) = error {
                let _ = direct_tx.send(Event::Error { message }).await;
            }
        }
    });

    // Whichever side finishes first ends the connection.
    tokio::select! {
        _ = &mut send_task => recv_task.abort(),
        _ = &mut recv_task => send_task.abort(),
    }

    state.leave(&room, &name).await;
    tracing::info!(%room, user = %name, "left");
}
