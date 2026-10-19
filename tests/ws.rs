//! End-to-end tests: start the real server on a random port and talk to it over WebSockets.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use live_chat_rust::{AppState, app};
use serde_json::Value;
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn start() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app(AppState::default()))
            .await
            .unwrap()
    });
    format!("127.0.0.1:{}", addr.port())
}

async fn join(addr: &str, room: &str, name: &str) -> Ws {
    let (ws, _) = connect_async(format!("ws://{addr}/ws?room={room}&name={name}"))
        .await
        .unwrap();
    ws
}

/// Next JSON event, skipping nothing. Fails the test if nothing arrives in time.
async fn next(ws: &mut Ws) -> Value {
    let msg = tokio::time::timeout(Duration::from_secs(2), ws.next())
        .await
        .expect("timed out waiting for event")
        .unwrap()
        .unwrap();
    serde_json::from_str(msg.to_text().unwrap()).unwrap()
}

/// Reads events until one of the given type shows up.
async fn next_of(ws: &mut Ws, kind: &str) -> Value {
    loop {
        let ev = next(ws).await;
        if ev["type"] == kind {
            return ev;
        }
    }
}

async fn send(ws: &mut Ws, text: &str) {
    ws.send(Message::Text(
        serde_json::json!({ "text": text }).to_string().into(),
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn messages_are_broadcast_to_the_room_only() {
    let addr = start().await;
    let mut alice = join(&addr, "general", "alice").await;
    assert_eq!(next(&mut alice).await["type"], "history");
    next_of(&mut alice, "join").await;

    let mut bob = join(&addr, "general", "bob").await;
    let mut eve = join(&addr, "other", "eve").await;
    next_of(&mut bob, "join").await;
    next_of(&mut eve, "join").await;

    let joined = next_of(&mut alice, "join").await;
    assert_eq!(joined["user"], "bob");
    assert_eq!(joined["online"], serde_json::json!(["alice", "bob"]));

    send(&mut bob, "  hello there  ").await;
    let msg = next_of(&mut alice, "message").await;
    assert_eq!(msg["user"], "bob");
    assert_eq!(msg["text"], "hello there");

    // eve is in another room and must not receive it
    assert!(
        tokio::time::timeout(Duration::from_millis(300), eve.next())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn late_joiners_get_history_and_leave_is_announced() {
    let addr = start().await;
    let mut alice = join(&addr, "dev", "alice").await;
    next_of(&mut alice, "join").await;
    send(&mut alice, "first").await;
    send(&mut alice, "second").await;
    next_of(&mut alice, "message").await;
    next_of(&mut alice, "message").await;

    let mut bob = join(&addr, "dev", "bob").await;
    let history = next(&mut bob).await;
    let texts: Vec<_> = history["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["text"].clone())
        .collect();
    assert_eq!(texts, vec!["first", "second"]);

    bob.close(None).await.unwrap();
    let left = next_of(&mut alice, "leave").await;
    assert_eq!(left["user"], "bob");
}

#[tokio::test]
async fn invalid_messages_return_errors() {
    let addr = start().await;
    let mut ws = join(&addr, "x", "taha").await;
    next_of(&mut ws, "join").await;

    ws.send(Message::Text("not json".into())).await.unwrap();
    assert_eq!(next_of(&mut ws, "error").await["type"], "error");

    send(&mut ws, &"a".repeat(501)).await;
    assert!(
        next_of(&mut ws, "error").await["message"]
            .as_str()
            .unwrap()
            .contains("too long")
    );
}

#[tokio::test]
async fn flood_protection() {
    let addr = start().await;
    let mut ws = join(&addr, "spam", "bot").await;
    next_of(&mut ws, "join").await;
    for i in 0..6 {
        send(&mut ws, &format!("msg {i}")).await;
    }
    let err = next_of(&mut ws, "error").await;
    assert!(err["message"].as_str().unwrap().contains("too fast"));
}

#[tokio::test]
async fn rejects_bad_names_and_lists_rooms() {
    let addr = start().await;
    assert!(
        connect_async(format!("ws://{addr}/ws?room=general&name=bad%20name"))
            .await
            .is_err()
    );

    let mut ws = join(&addr, "Lobby", "taha").await;
    next_of(&mut ws, "join").await;

    let body = http_get(&addr, "/api/rooms").await;
    let rooms: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(rooms, serde_json::json!([{ "name": "lobby", "online": 1 }]));
}

/// Tiny HTTP/1.1 GET so the tests don't need an HTTP client dependency.
async fn http_get(addr: &str, path: &str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n").as_bytes(),
        )
        .await
        .unwrap();
    let mut buf = String::new();
    stream.read_to_string(&mut buf).await.unwrap();
    buf.split("\r\n\r\n").nth(1).unwrap_or_default().to_string()
}
