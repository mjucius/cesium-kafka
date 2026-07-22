# Security Policy

## Reporting a vulnerability

Please report suspected vulnerabilities privately via GitHub Security Advisories
("Report a vulnerability" on the repository's Security tab). Do not open public issues for
security reports.

---

## Secure-deployment guide

cesium-kafka **delegates authentication and authorization to the Kafka broker**. It never implements
its own auth: it opens ordinary Kafka clients, and the broker decides who may connect and what each
client may do. The secure-deployment posture is therefore "configure the broker correctly and give
cesium least-privilege credentials" — the controls below are the operator's responsibility, and
several of them (the tracker write ACL, the `TransactionalId`/`Group` ACLs) are **normative
requirements**, not optional hardening.

The companion [docs/operations.md §5](docs/operations.md#5-least-privilege-deployment-tls-sasl-and-the-acl-matrix)
mirrors the key points next to the operational runbooks; [docs/design.md](docs/design.md) carries the
rationale (R12, §3.6).

### 1. Require TLS in transit and SASL authentication in production

cesium connects to the broker exactly as configured — including `PLAINTEXT` if you let it. For any
deployment that is not a single-host demo:

- **Encrypt in transit (TLS).** Set `security.protocol` to `SSL` or `SASL_SSL` so broker traffic
  (records, tracker state, offsets, credentials) is not on the wire in clear text.
- **Authenticate cesium to the cluster (SASL).** Use `SASL_SSL` with a real mechanism
  (`SCRAM-SHA-512`, `GSSAPI`/Kerberos, `OAUTHBEARER`, ...). cesium then connects as one identifiable
  principal the ACLs below can be scoped to.

All of this flows through the **`kafka.properties` passthrough** (applied to every cesium client —
consumers, producers, and the admin client) and the per-client overlays; cesium owns only the
correctness-critical keys (`isolation.level`, `auto.offset.reset`, the derived group/transactional
ids, ...) and never the security keys. **Keep secrets out of the config file** — reference them with
`${env:VAR}` so the JAAS password / keystore password come from the environment, never from a
committed YAML. Example (illustrative — substitute your mechanism):

```yaml
kafka:
  properties:
    bootstrap.servers: ${env:KAFKA_BOOTSTRAP}
    security.protocol: SASL_SSL
    sasl.mechanism: SCRAM-SHA-512
    ssl.truststore.location: /etc/cesium/tls/truststore.p12
    ssl.truststore.password: ${env:KAFKA_TRUSTSTORE_PASSWORD}
    sasl.jaas.config: ${env:KAFKA_JAAS_CONFIG}   # contains the SCRAM username + password
```

### 2. Least-privilege ACL matrix for all six client roles

All six cesium clients authenticate as the **single cesium SASL principal** (call it `User:cesium`)
supplied through `kafka.properties`. The six roles differ only in the **resources** they touch, so
least privilege means granting that one principal exactly the ACLs below and nothing else. Substitute
your `application-id` for `<app>` (e.g. `cesium.orders-delay.ingest`). Notes:

- **`DESCRIBE` is implied by `READ`/`WRITE`** on the same resource (Kafka ACL semantics), so it is not
  listed separately where a READ/WRITE is already granted.
- **Idempotent producer writes are authorized by topic `WRITE`** in Kafka ≥ 3.0 (the floor here is
  Kafka 4 — ADR-0017); no separate cluster `IdempotentWrite` grant is needed.
- A transactional producer that commits consumer offsets via `sendOffsetsToTransaction` needs
  **`READ` on the consumer `Group`** it commits for (the `AddOffsetsToTxn`/`TxnOffsetCommit`
  requirement), in addition to `WRITE` on its `TransactionalId`.

| # | Client role | Resource (type · name/pattern) | Operations | Why |
|---|---|---|---|---|
| 1 | **Ingest consumer** (group A) | `Group` · `cesium.<app>.ingest` (literal) | READ | join group, fetch/commit source offsets |
|   |  | `Topic` · *source* | READ | consume the source records |
| 2 | **Dispatch consumer** (group B) | `Group` · `cesium.<app>.dispatch` (literal) | READ | join group, fetch/commit the recovery cursor |
|   |  | `Topic` · *tracker* | READ | replay/stream scheduler state |
| 3 | **Seek consumer** (group-less) | `Topic` · *source* | READ | re-fetch payloads at dispatch time (pure `assign`/`seek`, **no `Group` ACL** — joins no group, commits no offsets) |
| 4 | **Ingest transactional producer** | `TransactionalId` · `cesium.<app>.ingest.*` (prefixed) | WRITE | open/commit the ingest transaction; **fencing control (M3)** |
|   |  | `Topic` · *tracker* | WRITE | produce schedule ADD records |
|   |  | `Topic` · *destination* | WRITE | `RELAY_IMMEDIATE` policy relays |
|   |  | `Topic` · *DLQ* | WRITE | malformed / over-max header-error records |
|   |  | `Group` · `cesium.<app>.ingest` | READ | `sendOffsetsToTransaction` commits group-A offsets |
| 5 | **Dispatch transactional producer** | `TransactionalId` · `cesium.<app>.dispatch.*` (prefixed) | WRITE | open/commit the dispatch transaction; **fencing control (M3)** |
|   |  | `Topic` · *destination* | WRITE | relay records at their due time |
|   |  | `Topic` · *tracker* | WRITE | completion tombstones |
|   |  | `Topic` · *DLQ* | WRITE | unfetchable-payload loss notices |
|   |  | `Group` · `cesium.<app>.dispatch` | READ | `sendOffsetsToTransaction` commits the cursor |
| 6 | **Admin client** | `Cluster` | DESCRIBE, DESCRIBE_CONFIGS | `describeCluster`, broker-config validation |
|   |  | `Topic` · *source*, *destination*, *tracker*, *DLQ* | DESCRIBE, DESCRIBE_CONFIGS | startup/periodic topic-config validation |
|   |  | `Group` · `cesium.<app>.ingest`, `cesium.<app>.dispatch` | DESCRIBE | first-run offset probe (`listConsumerGroupOffsets`) |
|   |  | `Cluster` (**`CREATE` bootstrap only**) | CREATE, ALTER | `createTopics` (tracker) + `createAcls` (apply the tracker ACL) |

> **The `TransactionalId` ACLs (rows 4 and 5) are load-bearing, not optional.** cesium's
> transactional and `group.instance.id` ids are fully deterministic
> (`cesium.<app>.<role>.<instance>.<ordinal>`) and the `application-id` seed is disclosed on the
> unauthenticated `/info` endpoint, so **without a `TransactionalId` ACL scoping
> `cesium.<app>.*` to the cesium principal, any authenticated co-tenant can open a producer with
> cesium's transactional id, call `initTransactions()`, and fence cesium's producer into a
> `ProducerFencedException` → fatal exit → restart → re-fence crash-loop** (finding **M3**). The same
> applies to the static `group.instance.id` (a `FencedInstanceIdException`). A single prefixed
> `TransactionalId` grant on `cesium.<app>.` covers both producers; restricting both consumer `Group`s
> is the companion control. **Do not stop at the tracker topic ACL** — that is the historical gap this
> matrix closes.
>
> If your security model forbids granting `Cluster:CREATE`/`Cluster:ALTER` to a long-lived relay,
> run `route.tracker.bootstrap: FAIL`, pre-provision the tracker topic and its write ACL out of band,
> and drop row 6's `CREATE`/`ALTER` grants entirely — the running cesium principal then needs only
> rows 1–5 plus the admin DESCRIBE grants.

### 3. Who may produce to source / read destination, DLQ, and tracker

Topic ownership splits cleanly; restrict each accordingly:

- **Source topic — your upstream producers write it; cesium only reads it.** cesium treats source
  records (key, value, headers, the `cesium-delay-ms` control header) as fully attacker-controlled and
  the parsers are hardened accordingly, but a hostile/multi-tenant source-producer population is still
  the surface behind findings **H1** (durable-backlog → recovery OOM), **M2** (oversized-record
  partition wedge), **L4** (flood/delay-bomb disk growth), and **L5** (the `FAIL` poison record). On
  shared ingress, restrict `WRITE` on the source to trusted producers and apply broker
  client/produce quotas and `retention.bytes` (see [No per-tenant rate limiting](#5-no-per-tenant-rate-limiting-l4) below).
- **Destination topic — cesium writes it; your downstream consumers read it.** Downstream consumers
  **must** use `isolation.level=read_committed` to observe exactly-once. Restrict `WRITE` on the
  destination to the cesium principal so nothing else can inject relay-looking records.
- **DLQ topic — cesium writes it; your DLQ-drain tooling reads it.** Restrict `WRITE` to the cesium
  principal; bound its retention (below).
- **Tracker topic — cesium writes and reads it; nothing else should.** This is the durable scheduler
  state. **Write access restricted to the cesium principal is a normative deployment requirement
  (R12).** A forged ADD record is a duplicate-injection primitive; a forged completion tombstone is a
  data-loss primitive. **cesium verifies this at startup** via `startup-checks.tracker-acl`: the
  default `WARN` surfaces a missing/foreign/unset grant (or a cluster with no authorizer to verify
  against) on every boot without blocking. **Set it to `FAIL` in production** to have cesium refuse to
  start unless the exclusive-write restriction is verifiably in force; `SKIP` omits the check for
  operators who enforce the restriction out of band. Ideally restrict `READ` as well — it is internal
  state, not a user surface.

### 4. Tracker integrity: the ACL is the v1 control; the invalid-records counter is not a forgery detector

The write-restricting tracker ACL (above) is the **v1 integrity control**. cesium verifies it on every
startup and reports a missing/foreign/unset grant; set `startup-checks.tracker-acl: FAIL` (default
`WARN`) to have a deployment missing the normative grant **fail closed** at startup instead of running
with an unauthenticated tracker channel. cesium provides **no in-band cryptographic integrity** on
tracker records in v1.

`cesium_tracker_invalid_records_total` is frequently useful but is **not** a tamper canary against a
capable adversary. It increments **only on structurally malformed or version-skewed writes** (bad
`0xC5` magic, unknown version, bad lengths) — the count-and-skip path. **It does not detect
well-formed forgeries.** A wire-format-aware adversary with tracker write access (trivially so once
this repository is public and `TrackerWireFormat`'s javadoc is visible) can craft an ADD or a
tombstone that decodes cleanly and is applied as **legitimate scheduler state** — a duplicate
injection or a silent data loss — and the counter never moves (finding **L2**).

Detecting *competent* tampering therefore requires controls outside cesium's wire format:

- **Broker authorizer audit logging** — who actually wrote the tracker topic — is the practical v1
  detector once the ACL is in place.
- **Record-level HMAC tamper-evidence** is a **reserved, deferred** option: the config namespace
  `store.kafka.hmac.*` is reserved but **not implemented in v1**. When delivered (a future release) it
  makes a forged record fail wire-format validation so it lands on the existing count-and-skip path. It
  only helps in hostile clusters where cesium's own credentials are already suspect (design R12); the
  **write-restricting ACL is the v1 control**, not HMAC.

### 5. No per-tenant rate limiting (L4)

cesium has **no notion of a tenant** — there is no per-producer, per-key, or per-partition rate limit
or fairness anywhere in ingest. Capacity and abuse isolation rely entirely on controls that sit
**before** cesium:

- **Broker client/produce quotas** on the source-producer principals.
- **Source `retention.bytes`** (size-based retention bounds what a flood can accumulate; note
  size/tier eviction interacts with the payload-lifetime check — `startup-checks.size-based-retention`).
- **Per-tenant topic isolation** (separate source topics / routes per tenant rather than one shared
  source).

Two sizing consequences worth budgeting for: **`delay.max` directly multiplies tracker disk** (the
tombstone-retention floor is `2 × delay.max` — see the
[operations sizing worksheet](docs/operations.md#2-the-tracker-sizing-worksheet)), and **the DLQ
should have its own retention bound** — cesium validates DLQ *existence* but not its retention, so a
malformed-record flood can fill an unbounded DLQ.

### 6. Observability port is unauthenticated (L1)

The observability HTTP server (default `observability.port: 8081`, serving `/metrics`, `/info`,
`/health/live`, `/health/ready`) is **read-only but completely unauthenticated** — no token, no mTLS,
no source-IP gate. `/info` and `/metrics` disclose the build version, the `application-id`,
per-partition operational gauges, and store capabilities (reconnaissance that also seeds the M3
fencing ids).

The listener **binds all interfaces by default** (`observability.bind-address: 0.0.0.0`) so k8s probes
reach it, and it now caps accepted connections (`jdk.httpserver.maxConnections`, default 64) on top of
the existing slow-client reaper, closing the M1 idle-socket exhaustion vector.

**The port MUST be network-restricted** — operators have no in-process auth to fall back on:

- **Set `observability.bind-address: 127.0.0.1`** to restrict the unauthenticated endpoints to a
  loopback sidecar scrape (the strongest and simplest control; recommended when off-host probes are
  not required).
- On Kubernetes where you need off-host probes, keep `0.0.0.0` and apply a **NetworkPolicy** scoping
  ingress to port 8081 to your monitoring namespace (the Prometheus scraper) only.

See [docs/operations.md §5](docs/operations.md#5-least-privilege-deployment-tls-sasl-and-the-acl-matrix)
and [§13](docs/operations.md#13-metrics-and-alerting).

### 7. Untrusted-producer ingress: avoid the `FAIL` delay policies (L5)

`delay.on-malformed-header: FAIL` and `delay.on-over-max: FAIL` (both **non-default**) are **unsafe
whenever the source topic has untrusted producers.** One crafted record (a non-decimal
`cesium-delay-ms`, an over-`delay.max` value, ...) fatally stops the ingest loop, tears down all
workers, and exits non-zero; because the offset never advances and `auto.offset.reset=none` is locked,
restart re-polls the same record and re-fails — a **pipeline-wide, restart-persistent outage**. Keep
the defaults (`DLQ`), or use `RELAY_IMMEDIATE`, for any multi-tenant ingress.
See [docs/configuration.md §5](docs/configuration.md#5-delay-protocol-delay) and
[docs/operations.md §5](docs/operations.md#5-least-privilege-deployment-tls-sasl-and-the-acl-matrix).

### 8. The quickstart compose is PLAINTEXT — not for production

[`config/docker-compose.yaml`](config/docker-compose.yaml) is the 5-minute quickstart: a single-broker
KRaft Kafka running **fully PLAINTEXT with no SASL/TLS/ACLs**, and topics created with no ACLs. It is a
local demo only — do **not** copy its broker posture into production. A production deployment needs
everything in this guide (TLS + SASL + the ACL matrix + the tracker write ACL).

---

For the normative ACL/credential requirement and the operational runbooks, see
[docs/operations.md](docs/operations.md); for the design rationale, [docs/design.md](docs/design.md)
(R12, §3.6).
