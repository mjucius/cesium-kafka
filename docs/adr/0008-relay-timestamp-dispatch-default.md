# ADR-0008: Relay timestamp = DISPATCH default

- **Status:** Accepted
- **Date:** 2026-06-06
- **Design reference:** [`../design.md`](../design.md) §2.4

## Context

When a delayed record is finally relayed, what timestamp should the destination record carry — the
original source `CreateTime`, or the dispatch instant? A delayed record can sit pending for hours
or a day (`delay.max` default `P1D`). Relaying it with its hours-old original timestamp can violate
the destination topic's `message.timestamp.difference.max.ms`, and it skews time-based retention
and any consumer that reasons about event time from the record timestamp. The original time must
nonetheless remain recoverable.

## Decision

`route.relay.timestamp` defaults to **`DISPATCH`** — the relay carries the dispatch instant (now).
`SOURCE` is an explicit opt-in for pipelines that need the original event time on the record
timestamp itself. In both modes the original source timestamp is preserved out-of-band in the
`cesium-source-timestamp` provenance header (default on), so `DISPATCH` never destroys information.

## Consequences

- Destination `message.timestamp.difference.max.ms` validation and time-based retention behave
  sanely for long-delayed records by default.
- The original event time is always recoverable from `cesium-source-timestamp`.
- `SOURCE` remains available where a downstream consumer keys off the record timestamp for event
  time; choosing it is the operator's explicit, documented decision.
