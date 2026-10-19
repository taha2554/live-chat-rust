use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use tokio::sync::{Mutex, broadcast};

pub const HISTORY_LEN: usize = 50;
pub const MAX_MESSAGE_LEN: usize = 500;

#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
    Message { user: String, text: String, ts: u64 },
    Join { user: String, online: Vec<String> },
    Leave { user: String, online: Vec<String> },
    History { messages: Vec<Event> },
    Error { message: String },
}

pub struct Room {
    pub tx: broadcast::Sender<Event>,
    pub history: VecDeque<Event>,
    pub users: Vec<String>,
}

impl Room {
    fn new() -> Self {
        let (tx, _) = broadcast::channel(128);
        Self {
            tx,
            history: VecDeque::with_capacity(HISTORY_LEN),
            users: Vec::new(),
        }
    }

    pub fn push_history(&mut self, event: Event) {
        if self.history.len() == HISTORY_LEN {
            self.history.pop_front();
        }
        self.history.push_back(event);
    }
}

#[derive(Clone, Default)]
pub struct AppState {
    pub rooms: Arc<Mutex<HashMap<String, Room>>>,
}

#[derive(Serialize)]
pub struct RoomInfo {
    pub name: String,
    pub online: usize,
}

impl AppState {
    /// Adds a user to a room (creating it if needed) and returns the name they
    /// actually got – duplicates get a numeric suffix – plus a receiver and the
    /// recent history.
    pub async fn join(
        &self,
        room: &str,
        wanted: &str,
    ) -> (String, broadcast::Receiver<Event>, Vec<Event>) {
        let mut rooms = self.rooms.lock().await;
        let room_state = rooms.entry(room.to_string()).or_insert_with(Room::new);

        let mut name = wanted.to_string();
        let mut n = 2;
        while room_state.users.contains(&name) {
            name = format!("{wanted}{n}");
            n += 1;
        }
        room_state.users.push(name.clone());

        let rx = room_state.tx.subscribe();
        let history = room_state.history.iter().cloned().collect();
        let _ = room_state.tx.send(Event::Join {
            user: name.clone(),
            online: room_state.users.clone(),
        });
        (name, rx, history)
    }

    pub async fn leave(&self, room: &str, user: &str) {
        let mut rooms = self.rooms.lock().await;
        let Some(room_state) = rooms.get_mut(room) else {
            return;
        };
        room_state.users.retain(|u| u != user);
        if room_state.users.is_empty() {
            // nobody left – drop the room and its history
            rooms.remove(room);
        } else {
            let _ = room_state.tx.send(Event::Leave {
                user: user.to_string(),
                online: room_state.users.clone(),
            });
        }
    }

    pub async fn say(&self, room: &str, user: &str, text: &str) {
        let event = Event::Message {
            user: user.to_string(),
            text: text.to_string(),
            ts: now_ms(),
        };
        let mut rooms = self.rooms.lock().await;
        if let Some(room_state) = rooms.get_mut(room) {
            room_state.push_history(event.clone());
            let _ = room_state.tx.send(event);
        }
    }

    pub async fn list(&self) -> Vec<RoomInfo> {
        let rooms = self.rooms.lock().await;
        let mut list: Vec<RoomInfo> = rooms
            .iter()
            .map(|(name, r)| RoomInfo {
                name: name.clone(),
                online: r.users.len(),
            })
            .collect();
        list.sort_by(|a, b| b.online.cmp(&a.online).then(a.name.cmp(&b.name)));
        list
    }
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Room and user names: 1–24 chars, letters, digits, '-' and '_'.
pub fn valid_name(s: &str) -> bool {
    let len = s.chars().count();
    (1..=24).contains(&len)
        && s.chars()
            .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
}

/// Trims the text and rejects empty or oversized messages.
pub fn clean_message(text: &str) -> Result<String, &'static str> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err("message is empty");
    }
    if trimmed.chars().count() > MAX_MESSAGE_LEN {
        return Err("message is too long (max 500 characters)");
    }
    Ok(trimmed.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names() {
        assert!(valid_name("taha_01"));
        assert!(valid_name("şehir"));
        assert!(!valid_name(""));
        assert!(!valid_name("has space"));
        assert!(!valid_name(&"a".repeat(25)));
    }

    #[test]
    fn messages() {
        assert_eq!(clean_message("  hi  ").unwrap(), "hi");
        assert!(clean_message("   ").is_err());
        assert!(clean_message(&"x".repeat(501)).is_err());
    }

    #[tokio::test]
    async fn duplicate_names_get_suffix_and_empty_rooms_are_removed() {
        let state = AppState::default();
        let (a, _rx1, _) = state.join("general", "taha").await;
        let (b, _rx2, _) = state.join("general", "taha").await;
        assert_eq!((a.as_str(), b.as_str()), ("taha", "taha2"));

        state.leave("general", &a).await;
        state.leave("general", &b).await;
        assert!(state.list().await.is_empty());
    }

    #[tokio::test]
    async fn history_is_capped() {
        let state = AppState::default();
        let (_name, _rx, _) = state.join("r", "u").await;
        for i in 0..(HISTORY_LEN + 10) {
            state.say("r", "u", &i.to_string()).await;
        }
        let (_n2, _rx2, history) = state.join("r", "v").await;
        assert_eq!(history.len(), HISTORY_LEN);
        assert!(matches!(&history[0], Event::Message { text, .. } if text == "10"));
    }
}
