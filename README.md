# cortex-bus-protocol

> **Command → Event → Query. CQRS for agent cognition.**

[![crates.io](https://img.shields.io/crates/v/cortex-bus-protocol.svg)](https://crates.io/crates/cortex-bus-protocol)
[![docs.rs](https://docs.rs/cortex-bus-protocol/badge.svg)](https://docs.rs/cortex-bus-protocol)
[![license](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A Rust library implementing a CQRS event-sourced message bus for agent systems. Commands express intent, Events record facts, Queries read projections. Full event replay, projection rebuilding, and time-travel debugging for multi-agent architectures.

---

## Table of Contents

- [What is CQRS + Event Sourcing?](#what-is-cqrs--event-sourcing)
- [Why Does This Matter?](#why-does-this-matter)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [API Reference](#api-reference)
- [Technical Background](#technical-background)
- [Installation](#installation)
- [Related Crates](#related-crates)
- [License](#license)

---

## What is CQRS + Event Sourcing?

**CQRS** (Command Query Responsibility Segregation) separates the write model (commands, events) from the read model (queries, projections). **Event Sourcing** stores every state change as an immutable event, enabling full replay and time-travel debugging.

```
┌──────────────────────────────────────────────────────────┐
│                     Write Side                           │
│                                                          │
│  Command ──► CommandHandler ──► Event ──► EventStore     │
│  (intent)    (validates)       (fact)    (append-only)   │
│                                                          │
├──────────────────────────────────────────────────────────┤
│                     Read Side                            │
│                                                          │
│  EventStore ──► Projection ──► Query ──► Result          │
│  (replay)      (read model)   (ask)     (answer)         │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

The key principles:

- **Commands** are imperative: "Do this thing" (may be rejected)
- **Events** are declarative: "This thing happened" (immutable facts)
- **Queries** ask questions of projections (derived read models)
- **Projections** are built from events (can be rebuilt at any time)

## Why Does This Matter?

**For agent debugging**: Every agent decision is recorded as an event. You can replay the full history, rebuild any past state, and answer "what went wrong?" with forensic precision.

**For multi-agent coordination**: Events are the shared truth between agents. Each agent maintains its own projections from the shared event stream — no race conditions, no distributed locks.

**For auditability**: In production agent systems, you need to know *why* an agent did something. Event sourcing gives you a complete audit trail: every command, every decision, every state change.

**For evolutionary architecture**: Projections are cheap to build and discard. Need a new dashboard view? Add a new projection from existing events — no data migration, no schema change.

## Architecture

```
cortex-bus-protocol
│
├── Command                    ← Intent to act
│   ├── command_type: String       Action name
│   ├── payload: String            Action parameters
│   └── id: u64                    Unique command ID
│
├── Event                      ← Fact that happened
│   ├── event_type: String         What happened
│   ├── payload: String            Event data
│   ├── seq: u64                   Monotonic sequence number
│   └── command_id: u64            Causality: which command caused this
│
├── Query                      ← Question to ask
│   ├── query_type: String         What to look up
│   └── params: HashMap            Filter parameters
│
├── EventStore                 ← Append-only event log
│   ├── new()                      Empty store
│   ├── append(type, payload, cmd) Add event, get sequence number
│   ├── replay_all()               Full event history
│   ├── replay_from(seq)           Events from sequence number
│   ├── len() / is_empty()         Store size
│   └── latest_seq()               Highest sequence number
│
├── CommandDispatcher          ← Route commands to handlers
│   ├── new()                      Empty dispatcher
│   ├── register(type, handler)    Register handler for command type
│   ├── dispatch(&Command)         Execute command → events
│   └── store()                    Access the event store
│
├── Projection (trait)         ← Read model interface
│   ├── apply(&Event)              Process an event
│   └── rebuild()                  Full state reset
│
├── CounterProjection          ← Example: counting events
│   ├── new()                      Zero counter
│   └── (counts events by type)
│
└── rebuild_projection()       ← Utility: rebuild from event store
    └── rebuild_projection(store, projection)
```

## Quick Start

```rust
use cortex_bus_protocol::{
    Command, Event, Query,
    EventStore, CommandDispatcher,
    CounterProjection, Projection, rebuild_projection,
};

// Create the event store and dispatcher
let mut dispatcher = CommandDispatcher::new();

// Register a command handler
dispatcher.register("create_task", |cmd, store| {
    vec![Event::new("task_created", &cmd.payload, store.latest_seq() + 1, cmd.id)]
});

// Dispatch a command
let cmd = Command {
    command_type: "create_task".into(),
    payload: "Build the exocortex".into(),
    id: 1,
};
let events = dispatcher.dispatch(&cmd);
println!("Events produced: {}", events.len());

// The event store now has the event
let store = dispatcher.store();
println!("Store has {} events", store.len());
println!("Latest sequence: {}", store.latest_seq());

// Replay events from the store
for event in store.replay_all() {
    println!("[{}] {} : {}", event.seq, event.event_type, event.payload);
}

// Build a projection (read model)
let mut counter = CounterProjection::new();
rebuild_projection(store, &mut counter);

// Query the projection
let query = Query::new("count").with_param("type", "task_created");
println!("Query: {:?}", query.params);
```

## API Reference

### Core Types

| Type | Fields | Description |
|------|--------|-------------|
| `Command` | `command_type`, `payload`, `id` | Intent to act |
| `Event` | `event_type`, `payload`, `seq`, `command_id` | Immutable fact |
| `Query` | `query_type`, `params: HashMap` | Question with filters |

### EventStore

| Method | Returns | Description |
|--------|---------|-------------|
| `new()` | `Self` | Empty store |
| `append(type, payload, cmd_id)` | `u64` | Add event, return sequence number |
| `replay_all()` | `&[Event]` | Full event history |
| `replay_from(seq)` | `Vec<&Event>` | Events from sequence number |
| `len()` | `usize` | Number of events |
| `is_empty()` | `bool` | No events |
| `latest_seq()` | `u64` | Highest sequence number |

### CommandDispatcher

| Method | Returns | Description |
|--------|---------|-------------|
| `new()` | `Self` | Empty dispatcher |
| `register(type, handler)` | `()` | Register command handler |
| `dispatch(&Command)` | `Vec<Event>` | Execute → produce events |
| `store()` | `&EventStore` | Access event store |

### Projection

| Method | Returns | Description |
|--------|---------|-------------|
| `apply(&Event)` | `()` | Process an event |
| `CounterProjection::new()` | `Self` | Example: event counter |
| `rebuild_projection(store, proj)` | `()` | Rebuild from all events |

### Query

| Method | Returns | Description |
|--------|---------|-------------|
| `new(query_type)` | `Self` | Create query |
| `with_param(key, value)` | `Self` | Add filter parameter |

## Technical Background

### CQRS Pattern

CQRS separates the write path (command → event) from the read path (query → projection):

**Why separate?**
- **Different optimization**: writes need consistency; reads need speed
- **Different scaling**: 1 write per transaction, 1000 reads per second
- **Different models**: normalized event stream vs. denormalized views

**For agents specifically**:
- Commands = what the agent wants to do
- Events = what actually happened
- Projections = the agent's current understanding
- Queries = asking about current state

### Event Sourcing

Instead of storing current state, store every state change:

```
State v3 = Event₁ + Event₂ + Event₃
```

Advantages:
- **Complete audit trail**: every change recorded
- **Time travel**: replay to any point in history
- **Debuggability**: reconstruct exact state at time of bug
- **No schema migration**: add new projections without changing events

### Event Ordering

Events are ordered by monotonically increasing sequence numbers:

```
Event₁ (seq=1) → Event₂ (seq=2) → Event₃ (seq=3) → ...
```

Each event records the command_id that caused it, maintaining causality:

```
Command(id=42) → Event(command_id=42) → Event(command_id=42)
```

### Projection Rebuilding

Projections are rebuilt by replaying all events in order:

```
projection.reset()
for event in store.replay_all():
    projection.apply(event)
```

This is idempotent: rebuilding twice produces the same state. Projections are disposable — you can always rebuild them from the event store.

### Command Handler Functions

Handlers are registered as function pointers:

```rust
type CommandHandlerFn = fn(&Command, &mut EventStore) -> Vec<Event>;
```

A handler receives the command and the event store, validates the command, and returns zero or more events. The dispatcher automatically appends these events to the store.

## Installation

```bash
cargo add cortex-bus-protocol
```

Or add to your `Cargo.toml`:

```toml
[dependencies]
cortex-bus-protocol = "0.1"
```

## Related Crates

Part of the **SuperInstance Exocortex** ecosystem:

- **[tiny-agent-protocol](https://github.com/SuperInstance/tiny-agent-protocol)** — 256-byte HTTP protocol for ESP32 agents
- **[categorical-coordination](https://github.com/SuperInstance/categorical-coordination)** — Category theory for multi-agent coordination
- **[shadow-cathedral](https://github.com/SuperInstance/shadow-cathedral)** — 3-layer shadow rendering pipeline
- **[signal-transduction](https://github.com/SuperInstance/signal-transduction)** — Signal cascading for agents
- **[dream-cycle](https://github.com/SuperInstance/dream-cycle)** — Sleep consolidation for agent memory

## License

MIT © [SuperInstance](https://github.com/SuperInstance)

Part of the [Exocortex](https://github.com/SuperInstance/exocortex) project.
