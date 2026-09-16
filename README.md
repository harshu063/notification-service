# Event Notification Service

A project-based High-Level Design (HLD) and implementation exercise for building a reliable, multi-channel event notification platform.

## Problem statement

Multiple upstream services produce business events such as order updates, payment failures, security alerts, and report completion. Each event may need to notify a webhook subscriber, an email recipient, or a mobile device.

The platform must accept up to **100,000 events per second at peak**, absorb bursts, isolate downstream failures, prevent unintended duplicate notifications, preserve ordering where the business requires it, and provide an explainable delivery history.

## Planned capabilities

- Accept events from multiple upstream services through HTTP and Kafka integrations.
- Validate and version event contracts.
- Route events using tenant subscriptions, preferences, policy, priority, and expiry.
- Deliver through webhooks, email, and mobile push notifications.
- Retry transient failures with bounded exponential backoff and jitter.
- Quarantine poison or terminal records in stage-specific dead-letter topics.
- Deduplicate producer retries, delivery intents, attempts, and provider callbacks.
- Preserve ordering within an explicit business key rather than globally.
- Expose event, delivery, attempt, audit, replay, and operational status.
- Provide an internal operator console for investigation and safe repair workflows.

## Proposed architecture

```text
Upstream services
       |
       v
Ingestion API / Kafka producers
       |
       v
Kafka: canonical accepted events
       |
       v
Routing, policy, preferences and template planning
       |
       +--------------------+--------------------+
       |                    |                    |
       v                    v                    v
Webhook intents        Email intents         Push intents
       |                    |                    |
       v                    v                    v
Webhook workers        Email workers         Push workers
       |                    |                    |
       +--------------------+--------------------+
                            |
                            v
              Attempt and status events
                            |
                            v
             PostgreSQL delivery read model
                            |
                            v
                  Operator console / API
```

Kafka is the durable asynchronous event backbone. PostgreSQL stores queryable state, configuration, deduplication records, and delivery lifecycle data. External providers remain at-least-once boundaries and therefore require stable identities, idempotent state transitions, and reconciliation where available.

## Technology direction

- Java and Spring Boot
- Spring for Apache Kafka
- Apache Kafka in KRaft mode for local development
- PostgreSQL with Flyway migrations
- Spring Data JPA/Hibernate initially, with JDBC/batching where profiling justifies it
- Docker Compose for the local Kafka/PostgreSQL environment
- TypeScript and React for the internal operator console
- Metrics, structured logs, traces, health checks, and audit records from the beginning

Technology choices will be pinned when implementation begins and recorded through Architecture Decision Records (ADRs).

## Delivery semantics

- A successful ingestion response means **durably accepted**, not delivered.
- Internal processing is designed for **at-least-once delivery**.
- Producers reuse the same event ID when retrying one logical event.
- Every logical notification receives a deterministic delivery ID.
- Consumers commit progress only after a durable, idempotent result.
- Provider acceptance, device/inbox delivery, and human engagement are separate states.
- Ambiguous provider timeouts are represented as unknown outcomes rather than guessed successes or failures.

## Deduplication

Deduplication occurs at several boundaries:

1. source event ID for producer retries;
2. deterministic delivery ID for routing retries;
3. conditional delivery-state transition before provider calls;
4. provider idempotency key when supported;
5. provider callback ID for repeated or out-of-order callbacks.

The deduplication retention window must cover the maximum retry, replay, Kafka retention, and provider-callback horizon plus a safety margin.

## Ordering

The system does not promise a global order. Events that require ordering carry:

- an explicit business ordering key;
- a source-issued sequence or business version;
- a Kafka record key derived from the tenant and ordering key.

Strict streams serialize work per key and define what happens when a prior event fails, expires, or never arrives. Relaxed streams may continue while preserving sequence metadata for stale-update protection.

## Implementation phases

1. **Requirements and contracts** — workload, SLOs, event envelope, identity, ordering, retry, security, and disaster-recovery decisions.
2. **Durable ingestion** — Spring Boot API, Kafka producer/consumer, PostgreSQL materialization, deduplication, status query, and tests.
3. **Webhook vertical slice** — subscriptions, signed delivery, SSRF protection, retries, DLQ, replay, and attempt timeline.
4. **Email and mobile push** — templates, preferences, provider adapters, callbacks, and token/address hygiene.
5. **Ordering and replay** — per-key sequence state, gap handling, retry scheduling, controlled replay, and audit.
6. **Scale validation** — representative workload generator and proof of the agreed 100k-events/s profile.
7. **Production readiness** — authentication, tenant isolation, privacy, observability, operations, and security testing.
8. **Regional recovery** — tested RTO/RPO, regional fencing, failover, reconciliation, and failback.

## Operator console

The planned UI is an internal operations and tenant-administration console. Its first read-only journey will let an authorized user:

- search events by tenant, event ID, type, source, ordering key, status, and time;
- inspect a chronological event and delivery timeline;
- distinguish accepted, submitted, delivered-if-known, suppressed, expired, failed, and unknown states;
- see duplicate counts and conflicting event identities;
- follow sanitized correlation information into observability tools.

Later phases add endpoint, subscription, template, DLQ, replay, provider-health, and audit workflows. High-impact commands require preview, scoped authorization, reason, idempotency, and immutable audit.

## Engineering principles

- Make every acknowledgement and failure boundary explicit.
- Put a stable identity on every retryable operation.
- Use Kafka for durable event transport and databases for query/coordination needs.
- Bound payload, fan-out, concurrency, retry, retention, replay, and queue age.
- Isolate tenant, provider, channel, priority, and endpoint failures.
- Measure oldest work age and drain time, not only queue length.
- Do not claim exactly-once delivery across external providers.
- Add operational repair, observability, security, and failure tests with each feature.
- Split deployable services only when scale, security, ownership, or fault isolation justifies the cost.

## Current status

The repository begins documentation-first. Application code should be added only after the initial contracts and architecture decisions are reviewed.

## Initial implementation milestone

The first milestone is a tested vertical path:

```text
POST event -> Kafka acknowledgement -> consumer materialization -> PostgreSQL -> status query
```

Completion requires unit and real Kafka/PostgreSQL integration tests for duplicates, conflicts, crashes, rebalances, unavailable dependencies, and eventual status visibility.

## License

No license has been selected yet. Until a license is added, the repository should be treated as all rights reserved by its owner.
