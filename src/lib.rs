//! # cortex-bus-protocol
//!
//! CQRS event-sourced agent bus with command handling, append-only event
//! store, and projection-based read models.

use std::collections::HashMap;
use std::fmt;

/// A command represents an intent to act.
#[derive(Debug, Clone, PartialEq)]
pub struct Command {
    /// Command type name.
    pub command_type: String,
    /// Payload data.
    pub payload: String,
    /// Unique command ID.
    pub id: u64,
}

/// An event represents a fact that happened.
#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    /// Event type name.
    pub event_type: String,
    /// Payload data.
    pub payload: String,
    /// Sequence number (monotonically increasing).
    pub seq: u64,
    /// ID of the command that produced this event.
    pub command_id: u64,
}

impl Event {
    /// Create a new event.
    pub fn new(event_type: &str, payload: &str, seq: u64, command_id: u64) -> Self {
        Self {
            event_type: event_type.to_string(),
            payload: payload.to_string(),
            seq,
            command_id,
        }
    }
}

/// A query against a read model.
#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    /// Query type name.
    pub query_type: String,
    /// Filter parameters.
    pub params: HashMap<String, String>,
}

impl Query {
    /// Create a new query.
    pub fn new(query_type: &str) -> Self {
        Self {
            query_type: query_type.to_string(),
            params: HashMap::new(),
        }
    }

    /// Add a parameter.
    pub fn with_param(mut self, key: &str, value: &str) -> Self {
        self.params.insert(key.to_string(), value.to_string());
        self
    }
}

/// Result of a query against a projection.
pub type QueryResult = HashMap<String, String>;

/// Append-only event store with replay capability.
#[derive(Debug, Clone)]
pub struct EventStore {
    events: Vec<Event>,
    next_seq: u64,
}

impl EventStore {
    /// Create an empty event store.
    pub fn new() -> Self {
        Self {
            events: Vec::new(),
            next_seq: 1,
        }
    }

    /// Append an event (auto-assigned sequence number).
    pub fn append(&mut self, event_type: &str, payload: &str, command_id: u64) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.events.push(Event::new(event_type, payload, seq, command_id));
        seq
    }

    /// Replay all events from the beginning.
    pub fn replay_all(&self) -> &[Event] {
        &self.events
    }

    /// Replay events starting from a given sequence number.
    pub fn replay_from(&self, seq: u64) -> Vec<&Event> {
        self.events.iter().filter(|e| e.seq >= seq).collect()
    }

    /// Number of events in the store.
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Check if the store is empty.
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Get the latest sequence number (0 if empty).
    pub fn latest_seq(&self) -> u64 {
        self.events.last().map(|e| e.seq).unwrap_or(0)
    }
}

impl Default for EventStore {
    fn default() -> Self {
        Self::new()
    }
}

/// A command handler that dispatches commands and produces events.
pub type CommandHandlerFn = fn(&Command, &mut EventStore) -> Vec<Event>;

/// A built-in command handler that creates a single event mirroring the command.
pub fn default_handler(cmd: &Command, store: &mut EventStore) -> Vec<Event> {
    let seq = store.append(&cmd.command_type, &cmd.payload, cmd.id);
    vec![Event::new(&cmd.command_type, &cmd.payload, seq, cmd.id)]
}

/// Dispatches commands to handlers and writes events to the store.
#[derive(Debug)]
pub struct CommandDispatcher {
    store: EventStore,
    handlers: HashMap<String, CommandHandlerFn>,
}

impl CommandDispatcher {
    /// Create a dispatcher with an empty store and no handlers.
    pub fn new() -> Self {
        Self {
            store: EventStore::new(),
            handlers: HashMap::new(),
        }
    }

    /// Register a handler for a command type.
    pub fn register(&mut self, command_type: &str, handler: CommandHandlerFn) {
        self.handlers.insert(command_type.to_string(), handler);
    }

    /// Dispatch a command. Falls back to the default handler if none registered.
    pub fn dispatch(&mut self, cmd: &Command) -> Vec<Event> {
        let handler = self
            .handlers
            .get(&cmd.command_type)
            .copied()
            .unwrap_or(default_handler);
        handler(cmd, &mut self.store)
    }

    /// Access the event store.
    pub fn store(&self) -> &EventStore {
        &self.store
    }
}

impl Default for CommandDispatcher {
    fn default() -> Self {
        Self::new()
    }
}

/// A projection builds a read model from an event stream.
pub trait Projection: fmt::Debug {
    /// Project name.
    fn name(&self) -> &str;
    /// Handle an event and update internal state.
    fn handle(&mut self, event: &Event);
    /// Query the current read-model state.
    fn query(&self, query: &Query) -> QueryResult;
}

/// A simple counter projection that counts events by type.
#[derive(Debug)]
pub struct CounterProjection {
    counters: HashMap<String, u64>,
}

impl CounterProjection {
    /// Create a new counter projection.
    pub fn new() -> Self {
        Self {
            counters: HashMap::new(),
        }
    }
}

impl Default for CounterProjection {
    fn default() -> Self {
        Self::new()
    }
}

impl Projection for CounterProjection {
    fn name(&self) -> &str {
        "counter"
    }

    fn handle(&mut self, event: &Event) {
        *self.counters.entry(event.event_type.clone()).or_insert(0) += 1;
    }

    fn query(&self, query: &Query) -> QueryResult {
        let mut result = HashMap::new();
        if query.query_type == "count" {
            if let Some(evt_type) = query.params.get("event_type") {
                if let Some(&count) = self.counters.get(evt_type) {
                    result.insert("count".to_string(), count.to_string());
                }
            } else {
                let total: u64 = self.counters.values().sum();
                result.insert("total".to_string(), total.to_string());
            }
        }
        result
    }
}

/// Replays events from a store through a projection.
pub fn rebuild_projection(store: &EventStore, projection: &mut dyn Projection) {
    for event in store.replay_all() {
        projection.handle(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_creation() {
        let cmd = Command { command_type: "create_user".into(), payload: "alice".into(), id: 1 };
        assert_eq!(cmd.command_type, "create_user");
        assert_eq!(cmd.id, 1);
    }

    #[test]
    fn event_creation() {
        let evt = Event::new("user_created", "alice", 1, 1);
        assert_eq!(evt.seq, 1);
        assert_eq!(evt.command_id, 1);
    }

    #[test]
    fn query_with_params() {
        let q = Query::new("count").with_param("event_type", "login");
        assert_eq!(q.params.get("event_type").unwrap(), "login");
    }

    #[test]
    fn event_store_append() {
        let mut store = EventStore::new();
        let seq = store.append("login", "alice", 1);
        assert_eq!(seq, 1);
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn event_store_replay_all() {
        let mut store = EventStore::new();
        store.append("a", "x", 1);
        store.append("b", "y", 2);
        assert_eq!(store.replay_all().len(), 2);
    }

    #[test]
    fn event_store_replay_from() {
        let mut store = EventStore::new();
        store.append("a", "x", 1);
        store.append("b", "y", 2);
        store.append("c", "z", 3);
        let from2 = store.replay_from(2);
        assert_eq!(from2.len(), 2);
    }

    #[test]
    fn event_store_latest_seq() {
        let mut store = EventStore::new();
        assert_eq!(store.latest_seq(), 0);
        store.append("a", "x", 1);
        assert_eq!(store.latest_seq(), 1);
    }

    #[test]
    fn event_store_is_empty() {
        let store = EventStore::new();
        assert!(store.is_empty());
    }

    #[test]
    fn dispatcher_default_handler() {
        let mut disp = CommandDispatcher::new();
        let cmd = Command { command_type: "test".into(), payload: "hello".into(), id: 42 };
        let events = disp.dispatch(&cmd);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].command_id, 42);
        assert_eq!(disp.store().len(), 1);
    }

    #[test]
    fn dispatcher_custom_handler() {
        fn handler(cmd: &Command, store: &mut EventStore) -> Vec<Event> {
            let s1 = store.append(&format!("{}_processed", cmd.command_type), &cmd.payload, cmd.id);
            let s2 = store.append(&format!("{}_logged", cmd.command_type), &cmd.payload, cmd.id);
            vec![
                Event::new(&format!("{}_processed", cmd.command_type), &cmd.payload, s1, cmd.id),
                Event::new(&format!("{}_logged", cmd.command_type), &cmd.payload, s2, cmd.id),
            ]
        }
        let mut disp = CommandDispatcher::new();
        disp.register("create", handler);
        let cmd = Command { command_type: "create".into(), payload: "item".into(), id: 1 };
        let events = disp.dispatch(&cmd);
        assert_eq!(events.len(), 2);
        assert_eq!(disp.store().len(), 2);
    }

    #[test]
    fn counter_projection() {
        let mut proj = CounterProjection::new();
        proj.handle(&Event::new("login", "a", 1, 1));
        proj.handle(&Event::new("login", "b", 2, 2));
        proj.handle(&Event::new("logout", "a", 3, 3));
        let result = proj.query(&Query::new("count").with_param("event_type", "login"));
        assert_eq!(result.get("count").unwrap(), "2");
    }

    #[test]
    fn counter_projection_total() {
        let mut proj = CounterProjection::new();
        proj.handle(&Event::new("a", "", 1, 1));
        proj.handle(&Event::new("b", "", 2, 2));
        let result = proj.query(&Query::new("count"));
        assert_eq!(result.get("total").unwrap(), "2");
    }

    #[test]
    fn rebuild_projection_from_store() {
        let mut store = EventStore::new();
        store.append("x", "", 1);
        store.append("x", "", 2);
        let mut proj = CounterProjection::new();
        rebuild_projection(&store, &mut proj);
        let result = proj.query(&Query::new("count").with_param("event_type", "x"));
        assert_eq!(result.get("count").unwrap(), "2");
    }

    #[test]
    fn projection_name() {
        let proj = CounterProjection::new();
        assert_eq!(proj.name(), "counter");
    }
}
