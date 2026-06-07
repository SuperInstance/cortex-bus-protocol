# cortex-bus-protocol

> **Command → Event → Query. CQRS for agent cognition.**

[![crates.io](https://img.shields.io/crates/v/cortex-bus-protocol.svg)](https://crates.io/crates/cortex-bus-protocol)
[![license](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

CQRS event-sourced message bus for agent systems. Commands express intent, Events record facts, Queries read projections. Full event replay and projection rebuilding.

## Architecture

```
Command ──► CommandHandler ──► Event ──► EventStore
                                           │
                                     ┌─────▼──────┐
                                     │ Projection  │
                                     │ (read model)│
                                     └─────┬──────┘
                                           │
                                       Query ──► Result
```

## Why CQRS for Agents?

Agent systems have fundamentally different read and write patterns:
- **Writes** are fast, append-only event streams (what happened)
- **Reads** need aggregated projections (current state, trends, dashboards)

CQRS separates these cleanly, enabling:
- Full event replay (time-travel debugging)
- Multiple projection views from the same events
- Audit trail of every agent decision

## Part of [Exocortex](https://github.com/SuperInstance/exocortex)

The **Nexus Bus** is the backbone of the exocortex — all inter-agent communication flows through CQRS channels.

## License

MIT © [SuperInstance](https://github.com/SuperInstance)
