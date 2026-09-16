# Event Notification Service — Project Overview

**Revision:** 1  
**Status:** Learning baseline / architecture proposal  
**Audience:** Developers learning High-Level Design (HLD), reviewers, product partners, SRE, security, and platform teams

## 1. The problem

Many business services produce events that people or external systems must know about: an order was shipped, a payment failed, a report is ready, a login looks suspicious, or a booking changed. If every upstream service implements email, push, and webhook delivery itself, the organization gets duplicated code, inconsistent retry behavior, uneven security, poor observability, and no single way to honor user preferences.

We will design and build a multi-tenant **Event Notification Service** that:

- accepts events from multiple upstream services;
- handles a peak ingress rate of **100,000 events per second**;
- evaluates notification rules, templates, subscriptions, and recipient preferences;
- delivers through **webhooks, email, and mobile push notifications**;
- absorbs traffic spikes without overloading downstream providers;
- retries transient failures and isolates permanent failures;
- prevents duplicate user-visible notifications as far as practical;
- preserves ordering where the business requires it;
- exposes delivery status, audit history, metrics, and operational controls.

This is not merely a “send an email” API. It is an asynchronous, failure-aware delivery platform between event producers and unreliable external providers.

## 2. Learning goals

By completing the project, a developer should learn how to:

1. turn an ambiguous idea into functional and non-functional requirements;
2. choose service boundaries and define contracts;
3. estimate throughput, storage, partitions, and provider capacity;
4. use Kafka producers, brokers, topics, partitions, consumer groups, and clusters;
5. reason about at-least-once delivery, idempotency, deduplication, and ordering;
6. design retries, exponential backoff, dead-letter handling, rate limits, and circuit breakers;
7. separate durable acceptance from downstream delivery;
8. design observability, security, privacy, multi-tenancy, disaster recovery, and operations;
9. explain trade-offs instead of presenting architecture as a collection of fashionable tools.

## 3. Scope

### In scope

- Event ingestion through a synchronous API and Kafka-based producer integration.
- Authentication, authorization, schema validation, quotas, and idempotency.
- Routing based on event type, tenant configuration, subscriptions, and user preferences.
- Template selection and rendering for channel/locale/version.
- Webhook, email, and mobile push delivery adapters.
- Per-provider throttling, retry scheduling, circuit breaking, and dead-letter queues.
- Deduplication, per-key ordering, delivery-state tracking, audit logs, replay, and observability.
- Multi-tenant isolation and a basic administration/control plane.

### Initially out of scope

- SMS, voice calls, chat applications, and in-app inbox (possible later channels).
- Marketing campaign design, audience segmentation, and journey automation.
- Exactly-once delivery to an end user or third-party provider; external side effects cannot generally be made exactly once.
- A global total order across all events.
- Content authoring UI beyond a minimal template/configuration API.

## 4. Actors and core use cases

| Actor | Use case |
|---|---|
| Upstream service | Publish a durable event with an event ID, type, tenant, subject, timestamp, payload, and ordering key. |
| Tenant administrator | Configure subscriptions, endpoints, templates, allowed event types, quotas, and secrets. |
| Recipient | Set channel, category, locale, quiet-hour, and opt-out preferences. |
| Operator/SRE | Observe lag and failures, pause a channel/tenant, replay eligible messages, rotate secrets, and inspect audit history. |
| Provider | Accept email or push requests and return synchronous/asynchronous delivery feedback. |
| Webhook subscriber | Receive signed HTTP callbacks and respond with an acknowledgement. |

Key flows:

1. **Accept event:** authenticate → validate → check quota/idempotency → durably append → acknowledge.
2. **Plan notifications:** consume event → find subscriptions/recipients → apply preferences/policy → create one delivery intent per target/channel.
3. **Render and dispatch:** resolve template → render immutable content → apply rate limit → call channel provider.
4. **Track result:** record attempt → process provider callback/response → retry, mark delivered, suppress, expire, or dead-letter.
5. **Operate:** search status, view metrics, pause/resume, repair configuration, and replay with authorization and audit.

## 5. Proposed high-level architecture

```text
                                       CONTROL PLANE
                          +------------------------------------+
                          | Tenant, subscription, template,    |
                          | preference and endpoint APIs       |
                          +----------------+-------------------+
                                           |
                                           v
+----------------+   HTTPS / Kafka   +--------------+      +--------------------+
| Upstream       | ----------------> | Ingestion &  | ---> | Kafka: raw events  |
| services       |                   | validation   |      | (partitioned)      |
+----------------+                   +--------------+      +----------+---------+
                                                                      |
                                                         consumer group|
                                                                      v
                                                        +-------------+---------+
                                                        | Routing / preference  |
                                                        | / policy engine       |
                                                        +-------------+---------+
                                                                      |
                                 +------------------------------------+----------------+
                                 |                                    |                |
                                 v                                    v                v
                        Kafka: webhook intents              Kafka: email       Kafka: push
                                 |                              intents             intents
                                 v                                    v                v
                        +--------+--------+              +------------+--+    +--------+-------+
                        | Webhook workers |              | Email workers |    | Push workers  |
                        +--------+--------+              +-------+-------+    +--------+-------+
                                 |                               |                     |
                                 v                               v                     v
                         Subscriber APIs                 Email provider(s)      APNs / FCM

Each worker path -> attempt/status events -> Kafka -> Delivery State Store / Audit / Analytics
                 -> retry topics or scheduler -> Dead-letter topic after terminal policy

Shared infrastructure: schema registry, dedupe/state store, secrets/KMS, cache,
rate limiter, observability, configuration store, and provider callback ingestion.
```

### Data plane versus control plane

- The **data plane** accepts and delivers high-volume events. It should keep functioning using cached/versioned configuration if the control plane is temporarily unavailable.
- The **control plane** manages tenants, templates, subscriptions, preferences, credentials, quotas, and operational commands. Its lower write volume should not be allowed to destabilize event delivery.

### Responsibilities by component

| Component | Responsibility |
|---|---|
| API gateway / ingress | TLS termination, authentication, request size limits, coarse quotas, WAF controls, and routing. |
| Ingestion service | Validate envelope/schema, authorize event type, enforce idempotency, select partition key, produce durably to Kafka, and return an event ID/receipt. |
| Schema registry | Own compatible event schemas and prevent accidental producer/consumer contract breakage. |
| Router / planner | Resolve subscriptions and recipients, apply policy/preferences, and emit immutable delivery intents. |
| Template renderer | Select a versioned template by tenant/event/channel/locale and render safely with deterministic inputs. |
| Channel workers | Execute provider-specific delivery, normalize errors, honor rate limits, and emit attempt/status events. |
| Retry scheduler | Delay transient retries without blocking normal partitions; apply exponential backoff plus jitter. |
| Delivery-state store | Provide queryable current status and attempt history; Kafka remains the event backbone, not the status-query database. |
| Dedupe store | Remember idempotency keys and/or delivery fingerprints for bounded retention. |
| Dead-letter handling | Quarantine terminal or poison records with reason, original metadata, and controlled replay workflow. |
| Observability stack | Metrics, structured logs, traces, dashboards, alerts, and business delivery SLIs. |

## 6. Kafka primer and why it belongs here

Kafka is a distributed append-only event log. It decouples producers from consumers, persists accepted events, allows independent consumer groups, provides ordering within a partition, supports replay, and absorbs bursts. Those properties directly address this project's peak traffic, fan-out, and failure-isolation needs.

### Core vocabulary

- **Producer:** application that writes records to a Kafka topic. Here, ingestion and planning services are producers.
- **Consumer:** application instance that reads records. A router or channel worker is a consumer.
- **Consumer group:** cooperating consumers sharing a topic; a partition is assigned to at most one consumer in a group at a time. This is the primary horizontal-scaling unit.
- **Topic:** named stream of related records, such as `notification.events.v1` or `notification.email.intent.v1`.
- **Partition:** ordered shard of a topic. Records with the same key are consistently routed to one partition, enabling per-key ordering.
- **Broker:** Kafka server storing partition replicas and serving reads/writes.
- **Cluster:** multiple brokers acting together. Production normally spans availability zones and uses replication.
- **Offset:** a record's monotonically increasing position inside a partition. Consumer progress is committed as offsets.
- **Replication factor:** number of replicas for each partition; commonly three across failure domains.
- **Leader/follower:** producers and consumers use a partition leader; followers replicate it and can take over.
- **Retention:** how long/large the log remains available for replay, independent of whether it has been consumed.

### Proposed topic families

Exact names and counts remain design decisions after capacity testing.

| Topic family | Purpose | Likely key |
|---|---|---|
| `notification.events.v1` | Validated canonical input events | `tenantId:aggregateId` |
| `notification.<channel>.intent.v1` | Delivery intents isolated by channel | ordering key or recipient/subscription ID |
| `notification.attempt.v1` | Immutable attempt results | delivery ID |
| `notification.status.v1` | Delivery lifecycle events; may feed compacted view | delivery ID |
| `notification.retry.<tier>.v1` | Backoff tiers or due work | original ordering key, with caveats |
| `notification.dlq.<stage>.v1` | Poison/terminal records by processing stage | original key |
| `notification.config.v1` | Versioned configuration change propagation | config entity ID |

### Producer posture

- Use `acks=all`, idempotent producer mode, bounded retries, delivery timeout, compression, and batching tuned through load tests.
- Require a stable event ID from the source or assign one at ingress and return it.
- Put ordering identity in the Kafka record key; never rely on arrival time for business order.
- Attach schema version, trace/correlation ID, tenant ID, occurred-at time, priority, and expiry.
- Treat a successful API acknowledgement as “durably accepted,” not “delivered.”
- Prefer an upstream transactional outbox where a source must atomically change its database and publish an event.

### Consumer posture

- Validate again at trust boundaries.
- Process a record, durably persist/emit its result, and only then commit the offset.
- Expect reprocessing after crashes or rebalances and make handlers idempotent.
- Bound concurrency, pause/resume partitions under downstream pressure, and monitor lag by topic/partition/group.
- Send poison records to a stage-specific DLQ after policy exhaustion so one record does not block a partition forever.
- Use cooperative rebalancing and graceful shutdown to reduce duplicate work during deployments.

### Brokers, clusters, partitions, and availability

- Run at least three brokers across three availability zones for the primary production cluster, subject to the organization's managed-Kafka standard.
- Use replication factor 3 and `min.insync.replicas=2` with producers using `acks=all`; this favors durability over accepting writes during a severe replica loss.
- Do not size partitions from peak events/second alone. Measure record size, ingress MB/s, egress fan-out, per-partition processing rate, consumer parallelism, replication, retention, and rebalance overhead.
- Partitions are not free: too few cap parallelism; too many consume broker memory/file handles, slow recovery/elections, and increase operational cost.
- Partition count generally cannot be reduced easily, and increasing it changes key-to-partition mapping. Preserve correctness through business sequence numbers, not an assumption that a key will always map to the same physical partition forever.
- Consider a second cluster/region only when recovery objectives justify the added cost and replication/failover complexity. Kafka replication does not automatically make databases, object stores, secrets, or provider callbacks consistent.

## 7. Event and delivery contracts

Illustrative canonical event envelope:

```json
{
  "eventId": "01J...",
  "eventType": "order.shipped",
  "schemaVersion": 1,
  "tenantId": "tenant-123",
  "source": "orders-service",
  "subject": { "type": "order", "id": "order-456" },
  "orderingKey": "tenant-123:order-456",
  "sequence": 42,
  "occurredAt": "2026-08-24T10:00:00Z",
  "receivedAt": "2026-08-24T10:00:00.120Z",
  "priority": "normal",
  "expiresAt": "2026-08-25T10:00:00Z",
  "traceId": "...",
  "data": {}
}
```

Important contract principles:

- `eventId` identifies one logical source event; a retry must reuse it.
- `orderingKey` defines the narrow entity whose order matters.
- `sequence` is source-issued and monotonic for that ordering key when strict gap/out-of-order detection is needed.
- `occurredAt` is business time; `receivedAt` is platform time. Neither alone guarantees order.
- Sensitive fields should be references or minimized data, not arbitrary payload copies.
- Schemas evolve with explicit compatibility rules; consumers tolerate additive fields.
- A delivery intent gets its own deterministic `deliveryId`, for example a hash/UUID derived from event + subscription + recipient + channel + template/policy version.

## 8. Deduplication strategy

Distributed delivery is normally **at least once**. Duplicate processing can occur when producers retry after a timeout, consumers crash after a side effect but before committing, providers accept a request but the response is lost, or operators replay data. Therefore, correctness comes from idempotency at multiple layers:

1. **Ingress dedupe:** require `eventId` or an idempotency key scoped by producer/tenant. Atomically record it with a bounded TTL and return the original receipt for identical retries. Reject key reuse with a different payload hash.
2. **Planning dedupe:** derive a stable `deliveryId` for each logical target. Insert delivery intent with a unique constraint/conditional write; an existing row means planning already occurred.
3. **Worker dedupe:** before calling a provider, atomically transition delivery state or acquire a short lease. Finished deliveries are no-ops on replay.
4. **Provider idempotency:** pass the stable delivery ID to providers that support idempotency keys. For webhooks, include it so subscribers can deduplicate.
5. **Status dedupe:** provider callbacks carry provider message/event IDs; record each callback once with a unique constraint.

The unavoidable edge case is a provider accepting a request while our worker times out before learning the result. Without provider idempotency or a reconciliation API, the system must choose between retrying (possible duplicate) and not retrying (possible loss). This business choice must be explicit per channel/event class.

Deduplication retention must be at least the maximum of event retention, replay window, retry horizon, and provider callback delay, plus safety margin. Infinite retention is rarely justified.

## 9. Ordering strategy

Do not promise global order. It destroys parallelism and is almost never a real business requirement. Promise order only within a clearly named scope, such as one order, account, user, or webhook subscription.

- Producers publish an `orderingKey`; Kafka uses it as the record key so related records enter the same partition.
- A source-issued `sequence` lets consumers detect duplicates, gaps, and stale records.
- Consumers serialize work per ordering key even if they process many different keys concurrently.
- Strictly ordered streams may hold event N+1 until N succeeds, expires, or is explicitly skipped/quarantined by policy.
- A retry topic can break order because later records continue while an earlier one waits. For strict streams, use a per-key state machine/queue or block that key only—not an entire busy Kafka partition.
- Channel ordering is not the same as event ordering. Email/mobile providers and networks may still deliver out of order after requests are issued. If visible order is critical, encode sequence/version in the client or collapse stale updates.
- Partition expansion, failover, replay, and multi-region ingestion require sequence-based correctness; partition order alone is insufficient.

## 10. Reliability and failure behavior

- **Transient failures:** retry with exponential backoff, full jitter, maximum attempts/age, and provider-aware `Retry-After` handling.
- **Permanent failures:** invalid address/token, rejected endpoint, bad template, or unauthorized request becomes terminal or requires configuration repair.
- **Poison events:** quarantine to stage-specific DLQ with error classification; do not endlessly retry malformed data.
- **Downstream outage:** buffer durably in Kafka, rate-limit drain after recovery, and protect healthy tenants/providers through bulkheads.
- **Provider degradation:** use timeouts, circuit breakers, per-provider concurrency pools, and optional provider failover where semantics permit.
- **Overload:** reject before durable acceptance with `429/503`, or accept and shed low-priority/expired work according to an agreed policy; never silently drop accepted work.
- **Zone failure:** Kafka replicas and stateless service instances span zones; quorum settings define the durability/availability trade-off.
- **Region failure:** invoke a tested runbook according to RTO/RPO. Avoid automatic dual-region sending unless duplicate and split-brain risks are solved.

## 11. Capacity framing

`100,000 events/second` is only the beginning of the estimate. Suppose an average event is 1 KiB and every event produces two delivery intents:

- raw payload ingress ≈ 100 MB/s before protocol/compression/replication;
- replication factor 3 means roughly 300 MB/s of broker replication write traffic, plus reads and overhead;
- two intents/event means 200,000 delivery intents/s;
- if average fan-out is 20 rather than 2, downstream work becomes 2,000,000 intents/s;
- one day at 100k events/s sustained is 8.64 billion events, so peak duration and daily average radically affect retention cost.

These are teaching examples, not final sizing. Requirements must establish average rate, peak duration, record-size percentiles, fan-out distribution, channel mix, priority mix, retention, retry amplification, and provider quotas. Then benchmark representative workloads and leave headroom.

## 12. Security and privacy baseline

- Authenticate services using mTLS or workload identity; authorize tenant, producer, and event type.
- Encrypt data in transit and at rest; store endpoint/provider secrets in a secrets manager backed by KMS.
- Sign webhooks with timestamped HMAC or asymmetric signatures; support secret rotation and replay protection.
- Apply SSRF defenses to webhooks: HTTPS policy, DNS/IP validation, block private/link-local/metadata networks, controlled redirects, egress proxy, and re-resolution protections.
- Minimize PII in Kafka and logs, redact secrets, define residency/retention/deletion behavior, and audit privileged operations.
- Enforce tenant-scoped queries, quotas, keys, and operational actions; test isolation explicitly.
- Sanitize templates, escape content by context, constrain payload size, and scan attachments/URLs if ever supported.

## 13. Observability and service objectives

Measure the user-visible pipeline, not just CPU:

- acceptance rate/errors/latency by producer and tenant;
- Kafka produce errors, partition skew, under-replicated partitions, consumer lag, rebalance rate, and oldest-record age;
- planned, suppressed, expired, attempted, delivered, failed, and DLQ counts;
- end-to-end time from durable acceptance to provider acceptance/delivery callback by channel and priority;
- retry distribution, provider response classes, rate-limit consumption, circuit state, and queue drain time;
- dedupe hits, sequence gaps, ordering holds, replay activity, and configuration version;
- cost per million accepted events and per successful notification.

Candidate SLIs/SLOs must be confirmed in requirements, for example:

- durable acceptance availability and p99 latency;
- percentage of high-priority notifications submitted to the provider within a target time;
- percentage of non-expired accepted intents reaching a terminal, explainable state;
- recovery time and data-loss objective by component/region.

Provider “accepted” is not necessarily inbox/device delivery. Status names must make that distinction clear.

## 14. Delivery plan for the learning project

1. **Foundation:** canonical schema, local Kafka, ingestion producer, one router consumer, status model, and load generator.
2. **Webhook vertical slice:** subscriptions, signed requests, timeouts, retry/backoff, dedupe, DLQ, and status query.
3. **Email and push adapters:** provider abstraction, templates, preferences, provider callbacks, and token/address hygiene.
4. **Ordering and replay:** keyed streams, sequence handling, per-key retry semantics, admin replay, and audit.
5. **Production hardening:** quotas, multi-tenancy, security review, observability, chaos/failure tests, capacity benchmarks, and DR exercise.

Each phase should end with architecture decision records (ADRs), automated contract/integration tests, dashboards, and a short failure-injection demonstration.

## 15. Engineering instincts and design principles

An experienced engineer should repeatedly ask:

1. **What is the actual business guarantee?** “Exactly once” and “ordered” are incomplete without a scope and failure boundary.
2. **Where is the durable handoff?** Know precisely when the producer may safely stop retrying.
3. **What happens after the timeout?** Unknown outcomes are normal in distributed systems.
4. **Is the operation idempotent?** Every retryable boundary needs an identity and state rule.
5. **What is the unit of isolation?** Tenant, provider, channel, priority, and ordering key should not create shared blast radius unnecessarily.
6. **Can backpressure travel upstream?** Queues buy time; they do not create infinite provider capacity.
7. **What is bounded?** Request size, queue age, retry count, concurrency, fan-out, retention, and cardinality must all have limits.
8. **Which state is authoritative?** Kafka log, configuration database, delivery-state database, and provider status serve different purposes.
9. **How will we operate and repair it?** Pause, inspect, replay, reconcile, rotate, drain, and audit are product features.
10. **What changes independently?** Channel adapters and contracts need versioning and isolation.
11. **Can we prove the capacity claim?** Benchmark the bottleneck and skewed keys, not only the happy-path average.
12. **Are we paying complexity rent too early?** Start with one region/cluster when requirements allow; add multi-region coordination only for a measured need.

## 16. Definition of success

The project succeeds when it can demonstrate, under a representative load test:

- durable ingestion at the agreed peak and burst duration with headroom;
- horizontal consumer scaling with controlled lag and partition distribution;
- no duplicate delivery in deterministic retry tests where the provider supports idempotency;
- documented behavior for ambiguous provider timeouts;
- correct per-key ordering under retries, crashes, and consumer rebalances for event classes that require it;
- tenant/provider isolation during throttling and outage scenarios;
- traceable delivery state from event ID to every channel attempt;
- tested backup, restore/replay, DLQ repair, and regional/zone failure runbooks consistent with agreed RTO/RPO;
- security controls for authentication, secret handling, webhook signing, SSRF, privacy, and audit.

The open decisions and the complete discovery questionnaire are maintained in `requirements.md`.
