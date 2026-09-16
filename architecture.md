# Implementable Architecture

**Revision:** 2  
**Date:** 2026-08-27  
**Status:** Accepted baseline for phased implementation

## 1. Architecture decision

Start with a **modular Spring Boot service** backed by Kafka and PostgreSQL. Preserve domain and adapter boundaries in code; split deployment units only when scaling, fault isolation, security, or team ownership produces evidence for it.

This revises the initial diagram's implication that every box must immediately be an independently deployed microservice. The logical target remains valid, but early distributed deployment would add network calls, schema coordination, deployments, and failure modes before we have validated notification semantics.

## 2. Why this shape

- The hardest early problems are event identity, acknowledgement semantics, deduplication, ordering, retries, and provider ambiguity—not service discovery.
- Kafka already provides the asynchronous boundary and independent consumer groups.
- Package/module boundaries give us extraction seams without paying the entire microservice cost.
- PostgreSQL is suitable for queryable state, unique constraints, transactional transitions, and a learning environment.
- Channel workers can be split later because each uses its own topic and normalized delivery contract.

## 3. Context architecture

```text
                         CONTROL PLANE
              subscriptions / templates / preferences
                                 |
                                 v
UPSTREAMS --> INGESTION --> KAFKA ACCEPTED EVENTS --> ROUTER / POLICY
                 |                                      |
                 |                                      +--> WEBHOOK INTENTS --> WEBHOOK WORKERS
                 |                                      +--> EMAIL INTENTS ----> EMAIL WORKERS
                 |                                      +--> PUSH INTENTS -----> PUSH WORKERS
                 |                                                               |
                 +--> receipt                                      external providers/endpoints
                                                                  |
                                                                  v
KAFKA ATTEMPT + STATUS EVENTS <--------------------------- normalized outcomes
                 |
                 v
       POSTGRESQL DELIVERY VIEW / AUDIT
```

Shared concerns are schema governance, identity/deduplication, secrets, rate limiting, retry scheduling, observability, tenant isolation, and operational repair.

## 4. Phase 1 runtime architecture

```text
                 +---------------------------------------------+
HTTP :8080 ----> | Spring Boot notification-service           |
                 |                                             |
                 | api -> application -> Kafka publisher      |
                 |                         |                   |
                 | Kafka consumer -> dedupe -> JPA repository |
                 +--------------------|------------------------+
                                      |
                          +-----------+-----------+
                          |                       |
                          v                       v
                   Kafka (KRaft)             PostgreSQL
                accepted-events log       materialized state
```

The HTTP API waits for Kafka producer acknowledgement. Kafka is therefore the Phase 1 durable acceptance boundary. PostgreSQL is updated asynchronously by an idempotent consumer.

### Important limitation

Ingress deduplication is not synchronous in Phase 1. A producer retry can append the same event to Kafka more than once, but consumer-side identity prevents duplicate materialized processing. Before real provider delivery, the delivery intent and dispatch layers must add a stable `deliveryId`, unique claim/state transition, and provider idempotency key where supported.

## 5. Logical modules and future extraction seams

| Module | Current responsibility | Extraction trigger |
|---|---|---|
| `api` | HTTP contracts, validation, error mapping | Independent edge scaling/security policy. |
| `application` | Use-case orchestration | Usually remains with owning runtime. |
| `domain` | Canonical event and status concepts | Shared as schemas/contracts, not a shared mutable library. |
| `messaging` | Kafka producer, consumer, topic declarations | Ingestion/router need independent scaling or ownership. |
| `persistence` | JPA entities and repositories | State workload or isolation requires a separate materializer. |
| future `routing` | subscriptions, policy, delivery-intent creation | Fan-out dominates ingress resources. |
| future `webhook` | signed HTTP dispatch and endpoint isolation | Endpoint latency/SSRF boundary deserves its own workers. |
| future `email` | provider-specific email submission/callbacks | Provider quotas and deliverability ownership. |
| future `push` | APNs/FCM submission and token hygiene | High throughput and provider-specific scaling. |

## 6. Data ownership

| Data | Authority | Query/read model |
|---|---|---|
| Canonical accepted event | Kafka topic during configured retention | PostgreSQL materialized record for status/search. |
| Processed event ID/hash | PostgreSQL unique primary key | Same table; used for idempotent materialization. |
| Subscription/template/preference | Future control-plane PostgreSQL schemas | Versioned cache/config events in data plane. |
| Delivery intent and attempt | Future PostgreSQL state machine plus Kafka lifecycle events | Delivery status API/audit view. |
| Provider callback | Immutable callback event + dedupe ID | Normalized delivery status. |

Kafka is not the query database. PostgreSQL is not the high-throughput event transport.

## 7. Event path and guarantees

1. Producer sends a canonical event with stable `eventId`, `orderingKey`, and optional source sequence.
2. API validates envelope and expiry, creates a canonical payload hash, and publishes using `tenantId:orderingKey` as Kafka key.
3. Kafka acknowledges under configured producer durability; API returns `202` with topic/partition/offset metadata.
4. Consumer group receives at least once.
5. Consumer inserts the event by unique `eventId` in a database transaction.
6. Same ID/hash is an idempotent no-op; same ID/different hash sets a conflict flag.
7. Offset is committed only after listener processing returns and the database transaction commits.

Phase 1 preserves order at Kafka partition level. Strict per-key gap handling and `lastCompletedSequence` state arrive before channel dispatch.

## 8. Kafka topology

### Local development

- one KRaft combined broker/controller;
- 12 partitions for `notification.events.v1` to make partition behavior visible;
- replication factor one because only one broker exists;
- plaintext listeners confined to the development machine/Compose network.

### Production target

- managed Kafka where available;
- at least three brokers distributed across three availability zones;
- replication factor three for durable topics;
- producer `acks=all`, idempotence enabled, `min.insync.replicas=2`;
- TLS, workload authentication/ACLs, quotas, zone-aware replica placement, backups/DR plan, and capacity for one broker/AZ loss;
- partition counts based on representative throughput, fan-out, processing rate, skew, recovery, and growth tests.

Topic families evolve as follows:

```text
notification.events.v1
notification.webhook.intent.v1
notification.email.intent.v1
notification.push.intent.v1
notification.attempt.v1
notification.status.v1
notification.retry.<tier>.v1
notification.dlq.<stage>.v1
notification.config.v1
```

## 9. PostgreSQL and ORM decision

Use Spring Data JPA/Hibernate for Phase 1 business state and learning velocity, with Flyway as the only schema-authoring mechanism. Hibernate schema mutation is disabled (`ddl-auto=validate`).

JPA is not assumed to be the final mechanism for every high-volume path. At later measured bottlenecks:

- use JDBC/batched writes for attempt firehoses;
- partition large tables by time/tenant where justified;
- move immutable analytical history to cheaper storage;
- keep unique constraints and conditional transitions for correctness;
- avoid loading large fan-out graphs as ORM aggregates.

## 10. Scaling model

- Ingestion instances scale horizontally; Kafka broker/partition capacity limits total production.
- Consumer instances scale up to the number of partitions in their group.
- Channel topics isolate provider latency and allow independent worker counts.
- Per-tenant, per-endpoint, per-provider, and per-priority bulkheads prevent noisy neighbors.
- Backlog is safe only within retention, expiry, storage, and recovery-drain capacity.
- A hot ordering key cannot be parallelized without relaxing its order; business key choice matters more than adding workers.

The 100k events/s goal is an eventual capacity gate, not a claim made by Phase 1. Before claiming it, measure peak duration, payload size, fan-out, retries, provider quotas, partition skew, and one-node/AZ failure behavior.

## 11. Availability and failover evolution

### Phase 1/local

Single instances deliberately expose core mechanics. No local HA claim is made.

### Production baseline

- stateless application instances across zones;
- multi-AZ Kafka and PostgreSQL;
- bounded timeouts/retries, graceful Kafka rebalances, liveness/readiness, and rolling-compatible schemas;
- provider/channel/tenant circuit breakers and bulkheads;
- warm secondary region initially, with explicit regional ownership and fencing before dispatch;
- tested recovery/failback for Kafka, schemas, PostgreSQL, configuration, secrets, callbacks, and DNS—not Kafka alone.

Active-active multi-region dispatch is deferred because it creates difficult ordering, duplicate, offset, and split-brain problems. Adopt it only if the agreed RTO cannot be met by active/warm-standby.

## 12. Security architecture

Phase 1 is intentionally local and has no production authentication. Before deployment outside a developer machine:

- authenticate workloads and authorize tenant/event type;
- TLS for APIs, Kafka, PostgreSQL, and providers;
- secrets through a managed secrets/KMS system;
- tenant-scoped data access and Kafka ACLs/quotas;
- payload classification, PII minimization, retention/deletion policy, and redacted logs;
- webhook signatures, replay protection, endpoint verification, strict SSRF/redirect/DNS defenses, and controlled egress;
- signed/authenticated provider callbacks and audited administration/replay.

## 13. Architecture fitness rules

- API acknowledgement must never be confused with provider delivery.
- Every retryable boundary has a stable identity.
- No external provider call occurs inside a database transaction.
- No unbounded payload, fan-out, retry, concurrency, retention, or replay.
- A channel outage cannot consume all ingestion/router resources.
- Kafka records are backward-compatible during rolling releases.
- Database migrations are additive/expand-contract across mixed versions.
- DLQ replay is previewed, authorized, rate-limited, idempotent, and audited.
- Microservice extraction needs a measurable trigger and an ADR.

