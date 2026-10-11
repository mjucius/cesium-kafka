# ADR-0018: Bounded wait for proven-existing topic metadata

- **Status:** Accepted
- **Amended by:** [ADR-0021](0021-store-spi-folded-into-core.md) (`CesiumEngine.buildRouteDescriptor` is gone; the engine takes the tracker topic id from startup validation instead of re-describing)
- **Date:** 2026-08-06
- **Design reference:** [`../design.md`](../design.md) §2.1 (tracker `CREATE` bootstrap), §7.6
  (startup checks); amended by [ADR-0020](0020-runtime-revalidation.md)

## Context

`StartupValidator`'s `CREATE` bootstrap created the tracker topic and then, as the next statement,
described it once, treating a single empty answer as proof the topic did not exist:

```java
admin.createTopic(topic, source.partitionCount(), configs);   // controller ack
...
Optional<TopicFacts> created = admin.describeTopic(topic);    // asked of a BROKER
if (created.isEmpty()) { /* "the cluster may be degraded" */ }
```

Those two calls are answered by different things. `CreateTopics` returns once the KRaft controller
commits the record to the metadata log. `DescribeTopics` is answered by a broker, from the
metadata image it has asynchronously published, on whichever node the AdminClient selects. A broker
that has not applied the record yet answers `UNKNOWN_TOPIC_OR_PARTITION`, and
`KafkaClusterAdmin.describeTopic` has to map that to `Optional.empty()`. At that layer it is
indistinguishable from a topic that does not exist.

The window is normally sub-millisecond, so the single-shot describe almost always passed. It failed
whenever the broker's metadata publisher stalled (CPU contention, a GC pause, a disk stall
creating the new partitions' log dirs). The result was a startup failure on a healthy
cluster, misreported to the operator as *"the cluster may be degraded"*.

It failed the nightly integration lane on 2026-06-24, 07-09, 07-20, 08-02
and 08-06, across six unrelated IT classes, always with that same message, always within a second of
process start.

## Decision

Wait, with a bound, only where the topic's existence is already proven. A new
`core.admin.TopicVisibility` polls on a capped-exponential backoff (50 ms doubling to a 1 s cap)
against a 10 s total budget, and is used only in the places where cesium already knows the topic
is there:

- the describe immediately after cesium's own `createTopic` (`StartupValidator.bootstrapTracker`);
- the `describeConfigs` calls that follow a successful `describeTopic`, because `describeConfigs` selects
  its node independently, so it can hit a laggier broker;
- `CesiumEngine.buildRouteDescriptor` / the IT harness's mirror of it, which re-describe topics that
  startup validation described successfully seconds earlier.

Existence checks for operator-provisioned topics keep failing fast. The source,
DLQ, destination, and the tracker pre-check under `bootstrap: FAIL` all still describe once. Retrying
there would only make cesium slower to tell an operator about a topic they forgot to
create, and the far more common cause of "topic missing" is a typo, not propagation lag.

Two corollaries follow from that asymmetry:

- The retry must not live in `KafkaClusterAdmin.describeTopic`. Every caller would inherit it,
  including the four fail-fast checks above. It lives in `TopicVisibility`, which callers opt into.
- The wait is never silent. When a wait actually occurs, validation emits a finding recording the
  milliseconds waited: informational below 1 s, a warning above it. A cluster whose propagation
  has degraded from sub-millisecond to seconds needs the operator's attention even though cesium
  started successfully.

Only a retriable metadata signal is retried: an empty describe, a description whose
partition count is still short of what was created (a `DescribeTopicPartitions` response is
cursor-paginated and reassembled client-side, so a partial reassembly must not be accepted as final),
or an `UnknownTopicOrPartitionException` from `describeConfigs`. Any other `ClusterAdminException`
(authorization, transport, timeout) propagates on the first occurrence instead of waiting out the
budget.

The budget is a constant, not a config key. It only has to exceed normal propagation, and a
misconfigured large value would hang a rollout. It can be promoted to `startup-checks.*` the first
time a deployment actually needs a different value.

## Consequences

- Startup fail-fast is softened in one specific way: an immediate ERROR becomes a bounded blocking
  wait of up to 10 s, on the failure path only, for topics cesium itself created. A doomed startup
  gets at most 10 s slower; a healthy one is unchanged (the first attempt still succeeds and costs no
  extra RPC).
- This introduces the first `Thread.sleep` in production source in this repository, on the
  startup thread (startup runs on the main thread). ADR-0020 adds a `cesium-admin` thread whose
  re-validation rounds use the same wait for topics startup proved exist; a deleted topic there costs
  one budget before it is reported, off the loop threads. There is no liveness impact (readiness is already false and no
  loops are running), and the interrupt path restores the flag and returns immediately, so a SIGTERM
  during startup is not converted into a cluster finding.
- `KafkaClusterAdmin.DEFAULT_TIMEOUT` (30 s) still applies to each individual call underneath, so the
  worst-case wall-clock is the budget plus one in-flight call timeout.
- The operator-facing error text no longer claims a degraded cluster. It names asynchronous metadata
  propagation, states the budget actually waited, and points at broker metadata lag or controller
  availability. See [`../operations.md`](../operations.md) §14.2.
- The production DLQ/source/destination variant is not fixed: a provisioning pipeline that creates
  a topic and starts cesium in the same second can still be told the topic does not exist. That is the
  accepted cost of keeping the typo case fast; the runbook tells operators to have provisioning wait
  for describability.

### Rejected alternatives

- **A blanket retry inside `KafkaClusterAdmin.describeTopic`.** One line, and it fixes the symptom,
  but it silently converts every fail-fast existence check into a 10 s wait, including the checks
  whose entire purpose is to tell an operator quickly that they forgot to create a topic.
- **Fixing it only in the test harness.** The harness half of the symptom (a DLQ topic created
  microseconds before `start()`) is a test artifact and is fixed there. But
  `bootstrapTracker` is production code on the production path; no test-side change can insert a wait
  between its `createTopic` and its `describeTopic`.
- **A `startup-checks.metadata-timeout` config knob.** Costs a record component, a validator rule, ten
  positional constructor call-site edits and four documentation surfaces, in exchange for a dial whose
  only misuse hangs a rollout.
