# Event Notification Service — Requirements and Architecture Discovery

**Revision:** 1  
**Status:** Draft for stakeholder discovery  
**Companion:** `overview.md`  
**Known headline:** Multiple upstream services; webhook, email, and mobile push; 100,000 input events/second at peak

## 1. Purpose of this document

This is both a requirements specification and a staff-engineer discovery guide. It separates:

- confirmed facts;
- proposed defaults;
- questions that require product/business/platform decisions;
- measurable acceptance criteria;
- architecture consequences and trade-offs.

Do not approve the HLD only because its diagram looks plausible. Approve it when the workload, delivery semantics, service objectives, failure behavior, security boundary, data lifecycle, operational model, and cost envelope are explicit.

## 2. Current facts, proposals, and unknowns

### Confirmed from the project brief

| ID | Statement |
|---|---|
| C-01 | The system consumes events from multiple upstream services. |
| C-02 | It supports webhook, email, and mobile notification channels. |
| C-03 | Peak input is 100,000 events per second. |
| C-04 | Kafka is the event backbone unless evidence shows it is unnecessary. |
| C-05 | The design must address topics, partitions, brokers, clusters, producers, consumers, deduplication, ordering, failover, and trade-offs. |

### Proposed defaults to validate

| ID | Proposal | Why it is not yet a requirement |
|---|---|---|
| P-01 | Asynchronous durable acceptance with at-least-once internal processing. | Business stakeholders must accept the semantics and status model. |
| P-02 | Per-tenant-and-aggregate ordering, not global ordering. | The true ordering scope is a domain decision. |
| P-03 | Kafka replication factor 3 across three zones, `acks=all`, and minimum two in-sync replicas. | Depends on managed platform, availability target, and cost. |
| P-04 | One primary region initially with a warm recovery region. | Must follow RTO/RPO and regulatory constraints. |
| P-05 | Separate Kafka intent topics and worker pools for webhook, email, and push. | Channel volumes, priorities, and isolation requirements may change topology. |
| P-06 | Stable source event ID plus deterministic delivery ID. | Producer capabilities and legacy integrations must be assessed. |
| P-07 | Delivery semantics stop at provider acceptance unless a provider supplies trustworthy callbacks. | Product language must distinguish provider acceptance from actual human receipt. |

### Critical unknowns

- Is 100k events/s a one-second burst, a 15-minute peak, or a sustained regional/global peak?
- What are average and p95/p99 payload sizes?
- How many recipients and channels does one event fan out to at p50/p95/p99/max?
- What percentage of traffic is webhook, email, or push, and what are provider quotas?
- Which notifications are transactional, security-critical, bulk, or best-effort?
- Which business entities require ordering, and what may happen when one event fails?
- What are the delivery latency SLOs, expiration windows, and end-to-end availability targets?
- What is the maximum replay window and how should replayed notifications be identified or suppressed?
- What data is PII/sensitive, where may it reside, and how quickly must it be deleted?
- What RTO/RPO and regional data-residency rules apply?
- What budget and operational staffing are available?

## 3. Requirement language and ownership

- **MUST** means required for release acceptance.
- **SHOULD** means expected unless an ADR documents why not.
- **MAY** means optional.
- Each requirement needs an owner, priority, validation method, and evidence link before implementation sign-off.
- Product owns user-visible behavior; domain producers own event meaning and source sequence; platform owns Kafka SLOs; notification service owns durable processing and provider submission; SRE owns operational readiness jointly with the service team; security/privacy approve relevant controls.

## 4. Functional requirements

### 4.1 Event ingestion and contracts

| ID | Requirement | Acceptance evidence |
|---|---|---|
| FR-ING-01 | The service MUST accept events from authorized upstreams through HTTPS and SHOULD allow trusted services to publish through an approved Kafka integration. | Contract and integration tests for both paths. |
| FR-ING-02 | Each event MUST include `eventId`, `eventType`, `schemaVersion`, `tenantId`, `source`, `occurredAt`, payload, and an explicit or derived ordering key. | Invalid envelopes are rejected/quarantined with a stable error code. |
| FR-ING-03 | The service MUST validate payload size, schema compatibility, tenant authorization, allowed event type, and timestamp bounds before routing. | Negative contract tests and security tests. |
| FR-ING-04 | API acknowledgement MUST mean the event is durably recorded; it MUST NOT imply notification delivery. | API documentation and failure-injection test. |
| FR-ING-05 | Producers MUST retry using the same event/idempotency ID. Reuse with a different canonical payload hash MUST be rejected as a conflict. | Retry and conflict tests. |
| FR-ING-06 | The platform MUST return/query a receipt containing event ID, acceptance status, and correlation ID. | API test. |
| FR-ING-07 | Schema evolution MUST follow a documented compatibility policy enforced before production publication. | CI compatibility check with schema registry. |
| FR-ING-08 | When a source changes a database and publishes the corresponding event, it SHOULD use a transactional outbox or equivalent atomic publication pattern. | Producer onboarding review. |

### 4.2 Routing, fan-out, policy, and preferences

| ID | Requirement | Acceptance evidence |
|---|---|---|
| FR-ROU-01 | The router MUST select subscriptions and targets by tenant, event type, attributes, and enabled channel. | Rule test matrix. |
| FR-ROU-02 | It MUST apply recipient consent, category opt-out, channel preference, locale, quiet hours, and legal suppression before intent creation or dispatch, as policy defines. | Policy tests including timezone/DST. |
| FR-ROU-03 | It MUST create one stable `deliveryId` per logical event/subscription/recipient/channel/policy version. | Reprocessing creates no duplicate logical intent. |
| FR-ROU-04 | It MUST enforce tenant, producer, event, recipient, and fan-out quotas. | Load/abuse tests. |
| FR-ROU-05 | It MUST support priority and expiration; expired work MUST not be sent and MUST end in an explainable state. | Queue-backlog test. |
| FR-ROU-06 | Configuration and template versions used for a delivery MUST be recorded for reproducibility. | Audit query. |
| FR-ROU-07 | Configuration failure MUST have defined behavior: use last-known-good version, reject/hold, or use an approved default; silent guessing is prohibited. | Dependency-outage test. |

### 4.3 Templates and content

| ID | Requirement | Acceptance evidence |
|---|---|---|
| FR-TPL-01 | Templates MUST be versioned by tenant/event/channel/locale with draft, validation, activation, rollback, and audit history. | Lifecycle integration test. |
| FR-TPL-02 | Rendering MUST be deterministic for the recorded template version and input payload. | Snapshot/replay test. |
| FR-TPL-03 | Missing variables, invalid markup, and oversized output MUST fail predictably before provider submission. | Negative rendering tests. |
| FR-TPL-04 | Rendering MUST escape untrusted data for the relevant context and MUST NOT execute arbitrary tenant code. | Security tests. |
| FR-TPL-05 | A fallback locale/template policy MUST be explicit per event class. | Locale matrix. |

### 4.4 Webhook delivery

| ID | Requirement | Acceptance evidence |
|---|---|---|
| FR-WEB-01 | Webhook requests MUST include delivery/event IDs, event type, schema version, timestamp, and signature metadata. | Consumer contract test. |
| FR-WEB-02 | Requests MUST use HTTPS and be signed over canonical content plus timestamp; secret rotation MUST support overlap. | Cryptographic verification/rotation test. |
| FR-WEB-03 | Subscribers MUST be told to deduplicate by delivery ID and return documented 2xx acknowledgement codes. | Public integration guide. |
| FR-WEB-04 | Connect/read/overall timeouts, maximum response size, redirect policy, and retryable status codes MUST be bounded and documented. | Fault-server tests. |
| FR-WEB-05 | The dispatcher MUST prevent SSRF, including private/link-local/metadata targets, unsafe redirects, and DNS rebinding paths. | Security test suite. |
| FR-WEB-06 | Endpoint verification and disablement after sustained permanent failure SHOULD be supported. | Lifecycle test. |
| FR-WEB-07 | Per-endpoint concurrency and rate limits MUST prevent one subscriber from exhausting shared workers. | Noisy-neighbor load test. |

### 4.5 Email delivery

| ID | Requirement | Acceptance evidence |
|---|---|---|
| FR-EML-01 | The adapter MUST submit transactional email with a stable provider/idempotency reference when supported. | Provider sandbox test. |
| FR-EML-02 | It MUST process provider callbacks for accepted, delivered, bounced, complained, deferred, and rejected states where available. | Signed callback tests. |
| FR-EML-03 | Permanent bounces/complaints MUST update suppression state according to policy. | End-to-end bounce test. |
| FR-EML-04 | Sender-domain controls (SPF, DKIM, DMARC), verified identities, unsubscribe obligations, and regional legal rules MUST be owned and monitored. | Deliverability/security readiness checklist. |
| FR-EML-05 | Provider failover MUST avoid duplicate sends and MUST be enabled only for event classes with an explicit ambiguity policy. | Failover game day. |

### 4.6 Mobile push delivery

| ID | Requirement | Acceptance evidence |
|---|---|---|
| FR-PSH-01 | The adapter MUST support APNs and FCM token/credential models required by the product. | Provider integration tests. |
| FR-PSH-02 | Invalid/unregistered device tokens MUST be deactivated or removed based on provider response. | Token hygiene test. |
| FR-PSH-03 | TTL, collapse key, priority, platform-specific payload limit, and notification/data-message behavior MUST be explicit. | Contract test by platform. |
| FR-PSH-04 | Sensitive content MUST NOT appear on a lock screen unless the product policy explicitly permits it. | Privacy review. |
| FR-PSH-05 | Provider acceptance MUST be reported separately from device display/open, which may be unknowable without client telemetry. | Status model review. |

### 4.7 Delivery lifecycle, retries, status, and replay

| ID | Requirement | Acceptance evidence |
|---|---|---|
| FR-LIF-01 | Each intent MUST have a documented state machine, such as `PLANNED → READY → ATTEMPTING → PROVIDER_ACCEPTED/RETRY_WAIT/FAILED/SUPPRESSED/EXPIRED/DLQ`. | State transition tests. |
| FR-LIF-02 | Attempts MUST record delivery ID, attempt number, provider, timestamps, normalized outcome, safe error details, and next action. | Audit query. |
| FR-LIF-03 | Transient retry MUST use exponential backoff with jitter, maximum attempts, maximum age, and provider `Retry-After` where valid. | Deterministic policy tests. |
| FR-LIF-04 | Permanent and poison failures MUST not retry indefinitely; they MUST reach a terminal or DLQ state. | Fault injection. |
| FR-LIF-05 | Status MUST be queryable by event ID and delivery ID subject to tenant authorization. | API tests. |
| FR-LIF-06 | Operators MUST pause/resume by channel/provider/tenant, replay an authorized bounded selection, and see predicted replay size. | Admin workflow and audit test. |
| FR-LIF-07 | Replay MUST preserve original identity and mark replay metadata; it MUST not bypass dedupe, consent, expiry, or policy unless a separately authorized mode explicitly says so. | Replay safety tests. |
| FR-LIF-08 | DLQ records MUST contain original topic/partition/offset, stage, schema, identity, safe error classification, attempt summary, and configuration versions. | DLQ inspection test. |
| FR-LIF-09 | Provider callbacks MUST be authenticated, idempotently consumed, and correlated to a delivery. | Duplicate/out-of-order callback tests. |

### 4.8 Administrative and operational functions

- Onboard/offboard tenants and producers; assign event-type permissions and quotas.
- Configure subscriptions, verified endpoints, templates, providers, credentials, priorities, and retry policy.
- Search sanitized audit and status records without exposing cross-tenant data.
- Pause/resume/drain workloads at an appropriately narrow scope.
- View lag, oldest work age, failure reasons, quota use, provider health, and estimated drain time.
- Perform controlled DLQ repair and replay with preview, approval for high-impact operations, rate limits, and immutable audit.
- Rotate credentials/signing secrets without downtime.

## 5. Non-functional requirements

Numerical targets below are **TBD until the questionnaire is answered**. Requirement gathering must produce exact numbers and measurement boundaries.

### 5.1 Performance and scale

| ID | Required decision/target |
|---|---|
| NFR-PERF-01 | Define whether 100k input events/s is global or per region, the peak duration, recurrence, and required capacity headroom. |
| NFR-PERF-02 | Define sustained average events/s and p50/p95/p99/max serialized event size. |
| NFR-PERF-03 | Define p50/p95/p99/max fan-out, channel mix, and retry amplification. |
| NFR-PERF-04 | Define API p50/p95/p99 durable-acceptance latency at normal and peak load. |
| NFR-PERF-05 | Define acceptance-to-provider-submission latency SLO by priority and channel; explicitly exclude/include quiet hours and provider outage. |
| NFR-PERF-06 | Capacity tests MUST include hot keys, slow providers, retries, consumer rebalance, broker loss, large messages, and backlog drain—not only uniform happy-path traffic. |
| NFR-PERF-07 | The system MUST enforce bounded payload, fan-out, concurrency, queue age, retry, and query cardinality limits. |

### 5.2 Availability, durability, and disaster recovery

| ID | Required decision/target |
|---|---|
| NFR-REL-01 | Define monthly durable-ingestion availability SLO and exclusions. |
| NFR-REL-02 | Define a separate delivery-processing availability/latency SLO per priority/channel. |
| NFR-REL-03 | Define RPO/RTO for zone loss, primary-region loss, Kafka cluster loss, delivery-state database loss, and configuration-store loss. |
| NFR-REL-04 | Accepted-event loss MUST be bounded by the approved RPO and measured through reconciliation. |
| NFR-REL-05 | Every dependency MUST have timeout, retry, circuit-breaker, bulkhead, and degraded-mode decisions. |
| NFR-REL-06 | Backups/restores and failover/failback MUST be tested on a schedule; documentation alone is insufficient. |
| NFR-REL-07 | Deployment MUST be backward compatible with in-flight records and support rollback without corrupting state. |

### 5.3 Consistency, idempotency, deduplication, and ordering

| ID | Requirement |
|---|---|
| NFR-CON-01 | The internal guarantee MUST be at-least-once processing with idempotent effects unless an ADR proves a different guarantee end to end. |
| NFR-CON-02 | Event ID uniqueness scope, payload-conflict behavior, dedupe-store consistency, and TTL MUST be defined. |
| NFR-CON-03 | Delivery ID derivation/versioning MUST remain stable across retries and ordinary replay. |
| NFR-CON-04 | Ordering scope MUST be named per event class, with source sequence rules and gap/stale-event behavior. |
| NFR-CON-05 | Strict-order streams MUST define whether later events block, skip, collapse, expire, or dead-letter when an earlier event fails. |
| NFR-CON-06 | The ambiguous provider-timeout policy MUST be selected per channel/event class: favor duplicate avoidance, favor delivery, or reconcile before retry. |
| NFR-CON-07 | Multi-region mode MUST define one writer/owner for each ordering key or an explicit conflict-resolution rule. |

### 5.4 Security, privacy, and compliance

| ID | Requirement |
|---|---|
| NFR-SEC-01 | Authenticate workloads and humans; authorize by tenant, role, producer, event type, channel, and operation. |
| NFR-SEC-02 | Encrypt transit and storage; use a managed secrets service/KMS; never put credentials in Kafka payloads or logs. |
| NFR-SEC-03 | Classify data fields; minimize PII; specify residency, retention, deletion, legal hold, audit, and subject-request behavior. |
| NFR-SEC-04 | Enforce webhook SSRF protection and signed replay-resistant requests. |
| NFR-SEC-05 | Enforce tenant isolation in storage keys/indexes, caches, Kafka authorization, queries, metrics/log access, and admin actions. |
| NFR-SEC-06 | Apply WAF/abuse controls, quotas, schema/size limits, template escaping, dependency scanning, patching, and incident response. |
| NFR-SEC-07 | Audit configuration, credential, replay, pause, deletion, and privilege operations immutably with actor and reason. |

### 5.5 Observability and operability

| ID | Requirement |
|---|---|
| NFR-OPS-01 | Propagate trace/correlation/event/delivery IDs without placing high-cardinality identifiers in unsuitable metric labels. |
| NFR-OPS-02 | Monitor Kafka throughput, produce latency/errors, ISR/replication health, disk, partition skew, consumer lag, and oldest-record age. |
| NFR-OPS-03 | Monitor end-to-end latency, success/failure/suppression/expiration, retries, DLQ, dedupe, ordering holds, provider quotas, and cost by safe dimensions. |
| NFR-OPS-04 | Alerts MUST be tied to user impact and have owner, severity, routing, runbook, and actionable thresholds. |
| NFR-OPS-05 | Operators MUST estimate backlog drain time and prevent a recovery surge from causing a second outage. |
| NFR-OPS-06 | Audit/replay/status tools MUST work during incidents and avoid dependency on the failing path where practical. |

### 5.6 Maintainability, evolvability, and quality

- Channel adapters MUST implement a small normalized interface while preserving provider-specific details needed for diagnosis.
- Contracts and topic schemas MUST be versioned, backward-compatible during rollout, and protected by CI tests.
- Services MUST support graceful shutdown, health/readiness, partition revocation handling, and safe rolling deployment.
- Infrastructure/configuration MUST be reproducible as code; manual production drift is audited and remediated.
- Unit, contract, integration, end-to-end, load, soak, chaos, security, and disaster-recovery tests MUST have owners and release criteria.
- ADRs MUST capture major decisions: Kafka topology, ordering scope, dedupe store, retry mechanism, provider strategy, state store, region strategy, and data lifecycle.

### 5.7 Cost and sustainability

- Establish monthly budget at average and peak traffic, plus a disaster/backlog-recovery scenario.
- Model Kafka storage/transfer, replicas, cross-region replication, databases, provider fees, logs/traces, and idle failover capacity.
- Track cost per million accepted events, planned intents, provider submissions, and successful outcomes.
- Put guardrails on unbounded retention, metric cardinality, replay, fan-out, template size, and tenant consumption.

## 6. Workload and capacity worksheet

Stakeholders must fill the following before final partition/broker sizing:

| Variable | Symbol | Required value |
|---|---:|---|
| Global peak input events/s | `E_peak` | Known headline: 100,000/s |
| Peak duration | `T_peak` | TBD |
| Average input events/s | `E_avg` | TBD |
| p50/p95/p99 event bytes | `S_event` | TBD |
| p50/p95/p99/max delivery fan-out | `F` | TBD |
| Webhook/email/push percentages | `C_web/C_email/C_push` | TBD |
| Retry amplification per channel | `R_channel` | TBD |
| Retention per topic family | `D_topic` | TBD |
| Growth over 12/24/36 months | `G` | TBD |
| Required headroom | `H` | TBD |
| Provider quotas and connection limits | `Q_provider` | TBD |
| Hot-key percentage/skew | `K_skew` | TBD |

Useful first-order calculations:

```text
input_bytes_per_second      = E_peak × S_event
delivery_intents_per_second = E_peak × F
channel_attempts_per_second = intents_for_channel × (1 + retry_amplification)
retained_logical_bytes      = average_bytes_per_second × retention_seconds
rough_replica_storage       = retained_logical_bytes × replication_factor × overhead_factor
drain_time                  = backlog / (safe_processing_rate - new_arrival_rate)
```

These formulas do not replace benchmarking. Compression, batch size, record headers, indexes, compaction, replication, cross-zone traffic, and uneven partitions all matter.

### Initial partition sizing method

For each topic, benchmark a conservative per-partition write rate and consumer processing rate using representative payloads. Then calculate:

```text
partitions_for_ingress = ceil(topic_peak_ingress / tested_safe_partition_ingress)
partitions_for_egress  = ceil(topic_peak_consumer_work / tested_safe_partition_processing)
initial_partitions     = max(partitions_for_ingress, partitions_for_egress,
                             required_consumer_parallelism) × headroom
```

Validate broker limits for partitions/replicas, disk throughput/capacity, network, CPU, file handles, controller recovery, replication catch-up, and failover with one broker unavailable. Never copy a partition count from an unrelated system.

## 7. Kafka requirements and integration design

### 7.1 Why Kafka is justified

Kafka is justified here if durable burst absorption, replay, multiple independent consumers, partition-key ordering, and high throughput are real needs. At 100k events/s with several downstream channels, those needs are credible. Kafka should not be treated as a job scheduler, query store, or magic exactly-once layer.

Reconsider Kafka only if discovery reveals that 100k is an extremely brief, lossy burst, downstream work requires no replay/ordering, and an existing managed queue meets all needs more simply. Such a decision requires an ADR and benchmark.

### 7.2 Cluster, broker, and availability requirements

- Production SHOULD use a managed Kafka cluster unless platform policy says otherwise.
- Primary cluster SHOULD span three availability zones, with replication factor 3 for durable topics.
- Proposed durability setting: producer `acks=all`, idempotence enabled, and topic/broker `min.insync.replicas=2`.
- Unclean leader election SHOULD be disabled for durable topics; availability must not silently override data integrity.
- Broker count is determined by throughput/storage/partition/failure benchmark plus N+1 capacity, not simply “three because replication is three.”
- During loss of any one broker/zone, remaining capacity SHOULD handle the agreed workload or explicitly shed/reject according to policy.
- Retention, segment size, compaction, quotas, ACLs, TLS, encryption, rack/zone awareness, and maintenance windows MUST be configured as code.
- Cluster ownership, patch/upgrade procedure, compatibility window, capacity alert thresholds, and escalation path MUST be documented.

### 7.3 Topic design requirements

Proposed logical topics:

| Topic | Cleanup | Key | Notes |
|---|---|---|---|
| `notification.events.v1` | delete/retention | `tenantId:orderingKey` | Canonical accepted events. |
| `notification.webhook.intent.v1` | delete | subscription/ordering key | Isolates webhook pressure and scaling. |
| `notification.email.intent.v1` | delete | recipient/ordering key | Isolates provider quotas. |
| `notification.push.intent.v1` | delete | recipient/ordering key | Handles APNs/FCM workloads. |
| `notification.attempt.v1` | delete | delivery ID | Append-only attempt history. |
| `notification.status.v1` | compact + delete | delivery ID | Latest status stream; authoritative query view is materialized separately. |
| `notification.retry.<tier>.v1` | delete | original key | Backoff buckets if selected; strict-order caveat applies. |
| `notification.dlq.<stage>.v1` | delete, longer retention | original key | Stage-specific quarantine and repair. |
| `notification.config.v1` | compact | entity ID | Distributes versioned config invalidation/state. |

For every topic, define owner, schema, producers, consumer groups, key, expected size/rate, partition count, replication factor, retention/compaction, PII class, ACL, maximum message bytes, DLQ, replay policy, SLO, and dashboard.

Do not create a topic per tenant or event type by default; that creates control-plane and broker metadata sprawl. Use shared topics with tenant keys/headers and enforce isolation through authorization, quotas, encryption, processing boundaries, and—only for justified high-risk tenants—dedicated infrastructure.

### 7.4 Partition and ordering requirements

- The record key MUST match the promised ordering scope; random keys destroy order, while tenant-only keys create hot partitions.
- All events for an ordering key MUST carry a source sequence when strict ordering matters.
- Consumers MUST detect sequence duplicates and define gap timeout/recovery behavior.
- Partition counts MUST support peak throughput and worker parallelism with failure headroom.
- Key distribution MUST be measured; dashboards MUST expose partition skew and hot keys.
- Increasing partitions changes mapping of keys to partitions. A migration/sequence strategy MUST prevent simultaneous processing or stale updates during transitions.
- A global total order is explicitly not required unless a new business requirement supplies its cost/latency justification.

### 7.5 Producer requirements

- Use an approved client version and shared configuration library where possible.
- Enable idempotent producer behavior, `acks=all`, bounded retries, delivery timeout, compression, and safe in-flight settings.
- Treat produce success as Kafka acknowledgement under configured durability, not downstream delivery.
- Use batching (`linger`/batch size) carefully: it improves throughput but adds latency.
- Classify retriable/non-retriable produce errors; apply bounded backoff; expose error metrics.
- Propagate trace metadata without exposing secrets/PII.
- Validate message size before produce; blob/large attachment content belongs in object storage with authorized references.
- For database-originated domain events, prefer a transactional outbox/CDC relay; a database commit followed by an unrelated Kafka call creates a dual-write loss window.
- Producer schemas MUST pass compatibility checks before deployment.

### 7.6 Consumer and consumer-group requirements

- Use distinct consumer groups for independent processing stages; replicas of one stage share one group.
- Maximum active consumers for a group is bounded by assigned partitions; extra consumers provide standby capacity but no processing parallelism.
- Disable blind auto-commit for effectful processing. Commit after a durable state/result transition.
- Handlers MUST tolerate redelivery and MUST use stable event/delivery identity.
- Use graceful shutdown and cooperative rebalancing where supported; stop accepting new records, finish/abort safely, commit, and release partitions.
- Bound poll batch and processing time; long work SHOULD be handed to a controlled async executor while preserving offset/ordering correctness.
- Pause/resume partitions or key queues for backpressure. Never let provider calls exhaust Kafka poll loops or unbounded memory.
- Monitor records/s, bytes/s, errors, lag per partition, oldest event age, processing latency, rebalance frequency/duration, retries, and DLQ.
- Poison records MUST follow a bounded retry/quarantine policy; they must not block an entire partition forever.

### 7.7 Kafka transactions and “exactly once”

Kafka transactions can atomically consume from Kafka and produce to Kafka together when correctly configured. They do **not** atomically include an email provider, mobile network, webhook subscriber, or ordinary database call. Therefore:

- Kafka transactions MAY be used for read-process-write stages if their operational cost is justified.
- External delivery remains at least once with idempotency/reconciliation.
- The architecture MUST not advertise “exactly-once notifications.”
- A database outbox/inbox, unique constraint, conditional state transition, or provider idempotency key is still required at non-Kafka boundaries.

### 7.8 Retry, DLQ, and ordering interaction

Three common retry mechanisms:

1. **Retry topics by delay tier:** scalable and observable, but later records may overtake the failed record and delay is coarse.
2. **Database/time-wheel scheduler:** precise due time and per-key control, but adds another stateful subsystem and high write/load requirements.
3. **In-partition pause:** preserves order but one failure can block unrelated keys sharing the partition and reduce throughput.

Recommended default: retry topics for unordered/best-effort notifications, and a per-key state/scheduler for event classes with strict order. The questionnaire must confirm this.

### 7.9 Multi-cluster and disaster recovery

- A secondary cluster is not a complete DR solution. Replicate or restore schemas, configuration, delivery state, dedupe state, secrets, templates, audit data, and routing ownership too.
- Define asynchronous replication RPO, topic-name mapping, offset/checkpoint strategy, identity preservation, and failback.
- Prevent both regions from dispatching the same logical delivery. Use explicit regional ownership/fencing and deterministic IDs.
- Planned failover and failback MUST be exercised. Measure actual RPO/RTO and duplicate/missing outcomes.

## 8. Deduplication requirements in detail

### 8.1 Identity hierarchy

| Identity | Scope | Purpose |
|---|---|---|
| `eventId` | Logical source event | Collapses producer/API retries. |
| `deliveryId` | Event + target/subscription + channel + relevant policy version | Collapses routing and worker retries. |
| `attemptId` | One provider call attempt | Audits attempts; does not represent logical uniqueness. |
| `providerMessageId` | Provider submission | Correlates callbacks/reconciliation. |
| `callbackEventId` | Provider callback event | Deduplicates repeated callbacks. |

### 8.2 Required behavior

1. Atomically claim the scoped event ID in a strongly consistent store or using a unique constraint.
2. Store canonical payload hash. Same ID/same hash returns original acceptance; same ID/different hash is a conflict and security/audit signal.
3. Derive and uniquely insert each delivery ID before dispatch.
4. Use conditional state transitions so only the current owner/lease can attempt delivery.
5. Pass delivery ID as provider idempotency key where supported and as an explicit webhook field/header.
6. Deduplicate callbacks by provider/callback ID and tolerate callback reordering.
7. Retain dedupe records at least through the longest replay/retry/callback horizon plus clock/operational margin.
8. Record dedupe hits, conflicts, TTL expiry, and store errors as metrics.

### 8.3 Trade-offs

- A strongly consistent dedupe store adds latency/cost but prevents concurrent duplicate claims.
- A probabilistic filter is useful as a cache/load reducer but false positives make it unsafe as the authority.
- Kafka log compaction helps retain latest values but is asynchronous and is not, by itself, an online uniqueness constraint.
- Longer TTL prevents replay duplicates but costs storage and complicates deletion/privacy obligations.
- Storing the full payload simplifies replay but expands PII risk; storing a hash plus durable canonical event reference is often safer.

## 9. Ordering requirements in detail

### 9.1 Questions every ordering claim must answer

- Ordered by what: source sequence, business version, or arrival?
- Ordered within what scope: aggregate, account, user, subscription, endpoint, tenant, or globally?
- Is the requirement processing order, provider-submission order, or user-visible presentation order?
- What happens to later events when an earlier event fails, expires, is suppressed, or never arrives?
- How long may the system wait for a sequence gap?
- Can state-update notifications be collapsed so only the latest version matters?
- Can different channels diverge in timing/order?
- Who owns the sequence and how is it fenced across source failover?

### 9.2 Proposed algorithm for strict per-key order

1. Producer assigns `orderingKey` and monotonically increasing `sequence` from the source-of-truth transaction/version.
2. Kafka producer uses `orderingKey` as record key.
3. Consumer reads partition order and checks a per-key `lastCompletedSequence` state.
4. `sequence <= lastCompleted` is a duplicate/stale no-op; `sequence == lastCompleted + 1` is eligible; a gap is held/reconciled according to timeout policy.
5. Only one active lease/fenced owner may execute a given key.
6. Completion and new sequence state are written conditionally/transactionally.
7. Failure blocks only that key when strict ordering is required; other keys continue.
8. Skip/expire/DLQ decisions create an explicit tombstone/outcome so the next sequence can proceed under policy.

### 9.3 Trade-offs

- Stronger ordering lowers throughput, increases tail latency, and expands failure coupling.
- Kafka guarantees order only inside one partition, not across topics/partitions/regions or at external providers.
- Keying too broadly creates hot partitions; keying too narrowly violates business order.
- Strict hold-on-failure may make one bad notification block a customer's entire stream.
- Relaxed order plus sequence/version in the client is often better for rapidly changing state notifications.

## 10. Failure-mode and failover requirements

| Failure | Detection | Required immediate behavior | Recovery/failover question |
|---|---|---|---|
| Upstream retry/timeout | Duplicate event ID/conflicting hash | Idempotently return original receipt or conflict | How long is producer retry permitted? |
| Ingestion instance loss | Health checks, error/latency | Load balancer routes to healthy stateless instances | Can remaining capacity handle peak? |
| Kafka broker/AZ loss | ISR, controller, produce latency | Replica leader election; honor durability quorum | Prefer rejecting writes or reducing durability? |
| Entire Kafka cluster/region loss | Synthetic probes, platform alert | Stop/redirect acceptance only under fenced DR plan | RTO/RPO and active-passive vs active-active? |
| Router crash/rebalance | Lag, heartbeat/rebalance metrics | Redeliver; dedupe intent; graceful ownership transfer | Max acceptable rebalance pause? |
| Dedupe/state DB loss | health/error/replica lag | Fail closed, buffer, or degrade by event class | Is duplicate risk preferable to rejected/delayed work? |
| Configuration store loss | errors/cache age | Use last-known-good version for bounded time or hold | Maximum config staleness? |
| Provider outage/throttle | error ratio, latency, 429/5xx | Circuit break, buffer, backoff, isolate provider | Fail over provider or wait? Duplicate ambiguity policy? |
| Webhook endpoint slow/down | timeout/5xx | Endpoint bulkhead and retry; no shared starvation | Disable after what duration/attempts? |
| Retry storm | attempt rate/backlog | Jitter, retry budgets, admission control, drain limits | Which priorities/tenants shed first? |
| Poison event/template | repeat deterministic error | Quarantine with reason; continue other work | Repair and replay authorization? |
| Hot partition/key | skew/lag | isolate/rate-limit key, diagnose key choice | Can key be subdivided without violating order? |
| State store corruption/restore | consistency checks | stop unsafe dispatch, restore/rebuild, reconcile | Source of truth and replay point? |
| Deployment/schema incompatibility | canary/error/DLQ | rollback, retain compatible consumers | Supported mixed-version window? |
| Secret compromise | security alert | revoke/rotate, pause affected endpoint/provider | Rotation propagation and forensic retention? |
| Operator replay mistake | replay audit/volume alerts | pause replay, limit blast radius | Approval and preview thresholds? |

For each row, assign owner, alert, runbook, automated response, manual decision, maximum detection time, recovery target, test method, and last exercise date.

## 11. Requirements questionnaire — select exactly one answer per question

The three choices expose architecture trade-offs. Choices marked **Recommended baseline** are a starting point for this project, not pre-approved facts. Record the selected letter, decision owner, rationale, and date. A stakeholder may refine wording later, but may not leave the semantics implicit.

### A. Product and functional semantics

#### Q1. What does a successful ingestion response mean?

- **A — Durably accepted (Recommended baseline):** event is replicated in the platform; delivery will happen asynchronously. Best throughput and failure isolation.
- **B — Delivery intent created:** wait for routing/policy planning. Stronger immediate feedback but more dependencies and latency.
- **C — Provider accepted:** wait until every provider submission. Simplest mental model for callers but unsuitable for large fan-out and provider outages.

#### Q2. Which notification classes exist?

- **A — Priority classes (Recommended baseline):** critical/transactional/bulk have distinct SLO, expiry, quotas, and retry budgets. More policy work, better isolation.
- **B — Transactional and bulk only:** moderate complexity; may be enough initially.
- **C — One class:** easiest implementation; a bulk surge can harm security-critical delivery.

#### Q3. How is event fan-out determined?

- **A — Subscription + preferences (Recommended baseline):** central router resolves configured targets and user policy. Flexible but state/cache intensive.
- **B — Producer supplies all targets:** simpler platform routing but couples producers to channels and preferences.
- **C — One fixed target per event:** simplest and highest throughput, but does not meet broad notification-platform needs.

#### Q4. When should preferences be evaluated?

- **A — At planning time:** reproducible intent and lower dispatch latency, but later preference changes may not affect queued work.
- **B — At dispatch time (Recommended baseline for non-critical messages):** honors latest preference, adds dependency/cache and may change replay result.
- **C — At both stages:** early suppression plus final policy check; safest but more complexity and requires recorded versions.

#### Q5. What should happen when a required template/configuration is unavailable?

- **A — Hold/retry then DLQ (Recommended baseline):** never send guessed content; increases latency.
- **B — Use last-known-good:** maintains delivery during control-plane outage; risks stale content.
- **C — Use generic fallback:** highest availability; may be legally or contextually wrong.

#### Q6. What is the replay contract?

- **A — Same identity and dedupe applies (Recommended baseline):** safe diagnostics/reprocessing without duplicate sends; cannot intentionally resend.
- **B — New delivery identity:** intentional resend is possible; duplicate user impact must be approved and labelled.
- **C — Dry-run only:** safest operationally; cannot recover genuinely missed deliveries.

### B. Scale and latency

#### Q7. What does 100,000 events/s mean?

- **A — Sustained per region:** most demanding; requires large regional capacity and provider planning.
- **B — Sustained global (Recommended planning assumption until measured):** shard across regions but coordinate tenancy/residency.
- **C — Short global burst:** Kafka buffers burst; size downstream for drain-time SLO.

#### Q8. How much capacity headroom is required at forecast peak?

- **A — 100% headroom:** strongest resilience/growth, highest idle cost.
- **B — 50% headroom (Recommended baseline):** balanced if autoscaling and quotas are proven.
- **C — 20% headroom:** lower cost, small safety margin during failure or forecast error.

#### Q9. What latency should high-priority notifications target from acceptance to provider submission?

- **A — p99 under 1 second:** expensive, sensitive to provider/network behavior.
- **B — p99 under 5 seconds (Recommended baseline):** strong transactional target with batching room.
- **C — p99 under 60 seconds:** cheaper and easier; not suitable for OTP/security-like use cases.

#### Q10. How should overload before durable acceptance behave?

- **A — Return 429/503 with retry guidance (Recommended baseline):** honest and bounded; producers must retry safely.
- **B — Accept into an alternate overflow store:** higher availability, adds reconciliation/order complexity.
- **C — Sample/drop low priority:** protects system but allowed only when the event class is explicitly lossy and metrics expose drops.

### C. Delivery guarantees, deduplication, and ordering

#### Q11. What internal delivery guarantee is required?

- **A — At least once + idempotency (Recommended baseline):** realistic across external effects; requires dedupe design.
- **B — At most once:** avoids duplicates but accepts lost notifications on crashes/timeouts.
- **C — Best effort:** lowest cost; only appropriate for explicitly non-critical traffic.

#### Q12. Who assigns the event ID?

- **A — Source assigns stable ID (Recommended baseline):** retries dedupe across network uncertainty; producer onboarding is stricter.
- **B — Ingestion assigns ID:** easier producers; duplicate API retries cannot always be recognized.
- **C — Derive from payload hash:** convenient for identical content but may merge legitimate repeated events and is sensitive to canonicalization.

#### Q13. How long are ingress dedupe keys retained?

- **A — Retry/replay horizon + margin (Recommended baseline):** requirements-driven balance of safety and cost.
- **B — Fixed 24 hours:** simple, but unsafe if retries/replays occur later.
- **C — Forever:** maximal duplicate protection, unbounded cost and deletion/privacy complications.

#### Q14. What is the required ordering scope?

- **A — Per aggregate/subscription key (Recommended baseline):** preserves relevant order with parallelism.
- **B — Per tenant:** stronger but hot tenants serialize and become bottlenecks.
- **C — Global:** simplest semantic statement, prohibitively limits scale and availability.

#### Q15. What happens when sequence N fails but N+1 is ready?

- **A — Block only that key (Recommended for strict classes):** preserves order without blocking unrelated keys; needs per-key state.
- **B — Allow N+1 and mark out of order:** high availability; consumers/UI must tolerate versions.
- **C — Block the Kafka partition:** easiest strict behavior, but unrelated keys suffer head-of-line blocking.

#### Q16. How are source sequence gaps handled?

- **A — Hold for bounded time, reconcile, then explicit skip/DLQ (Recommended baseline):** detectable and operable, adds state/latency.
- **B — Process immediately:** lowest latency, ordering is only best effort.
- **C — Hold forever:** strongest apparent order, unbounded stuck state and operational risk.

#### Q17. Provider accepted the request but our response timed out. What next?

- **A — Retry with provider idempotency key (Recommended where supported):** best outcome if provider semantics are reliable.
- **B — Reconcile before retry:** minimizes duplicates, increases latency and needs provider query support.
- **C — Choose per event class between retry and stop:** necessary when provider has no idempotency/query; explicitly trades duplicates against loss.

### D. Kafka topology and processing

#### Q18. How should channel work be isolated?

- **A — Topic and worker pool per channel (Recommended baseline):** clear bulkheads/scaling; more topics and operations.
- **B — One shared intent topic:** simpler topology; slow providers can cause interference unless scheduling is sophisticated.
- **C — Separate Kafka cluster per channel:** strongest isolation, high cost and operational overhead.

#### Q19. What durability should primary Kafka topics use?

- **A — RF=3, `acks=all`, min ISR=2 (Recommended baseline):** tolerates one replica loss without silent durability reduction; may reject writes during severe failure.
- **B — RF=3, `acks=1`:** lower latency/higher availability, acknowledged records can be lost on leader failure.
- **C — RF=2, `acks=all`:** lower cost, less fault tolerance and maintenance flexibility.

#### Q20. How are partitions selected?

- **A — Benchmark throughput/processing and add headroom (Recommended baseline):** defensible and workload-specific.
- **B — Match current consumer instance count:** easy initially, tightly couples topology and deployment.
- **C — Pick a large round number:** quick, risks metadata/recovery overhead or still being insufficient.

#### Q21. What record key should canonical events use?

- **A — Tenant + business ordering key (Recommended baseline):** per-entity order and cross-tenant separation; must monitor skew.
- **B — Event ID:** uniform distribution, loses business-key ordering.
- **C — Tenant ID only:** tenant order but large tenants become hot partitions.

#### Q22. How are offsets committed?

- **A — After durable idempotent result/state (Recommended baseline):** safe redelivery, more implementation work.
- **B — Before processing:** avoids duplicate processing, can lose work on crash.
- **C — Periodic auto-commit:** simplest, creates both loss and duplicate ambiguity around side effects.

#### Q23. Which retry mechanism should be used?

- **A — Hybrid by ordering class (Recommended baseline):** retry topics for relaxed order; per-key scheduler/state for strict order. More components, correct semantics.
- **B — Retry topics for everything:** scalable/simple; breaks strict order.
- **C — Pause original partitions:** preserves partition order; head-of-line blocking and reduced throughput.

#### Q24. How long should canonical events remain in Kafka?

- **A — Based on replay/RPO/cost policy (Recommended baseline):** e.g. days, with longer archive if justified.
- **B — 24 hours:** low storage, narrow repair window.
- **C — Indefinitely:** maximum replay, high cost/privacy/operational burden; Kafka becomes an archive by accident.

### E. Failover and degradation

#### Q25. What regional availability model is required?

- **A — Single active region + warm standby (Recommended baseline absent stricter SLO):** simpler ownership/order, non-zero failover RTO/RPO.
- **B — Active-passive hot standby:** faster recovery, substantial duplicated cost and failover automation.
- **C — Active-active:** best regional availability potential, hardest dedupe/order/fencing/state problem.

#### Q26. During one Kafka replica/AZ loss, should ingestion continue if the durability quorum cannot be met?

- **A — Reject until quorum returns (Recommended baseline):** protects acknowledged-event durability, reduces availability.
- **B — Temporarily lower min ISR/acks:** maintains writes, accepts data-loss risk.
- **C — Divert to an independent overflow log:** may preserve both, adds order/reconciliation/operations complexity.

#### Q27. What happens when the delivery-state/dedupe store is unavailable?

- **A — Stop dispatch and buffer in Kafka (Recommended baseline for critical work):** avoids uncontrolled duplicates; increases delay/backlog.
- **B — Dispatch without dedupe state:** preserves latency, risks duplicate notifications and inconsistent status.
- **C — Degrade by class:** stop critical/strict work, allow explicitly best-effort traffic; complex but business-aware.

#### Q28. How should provider outage failover work?

- **A — Buffer and wait for primary (Recommended default):** minimizes duplicates/semantic drift, increases latency.
- **B — Automatically switch provider:** faster recovery, risks duplicates, sender differences, callbacks, and inconsistent capabilities.
- **C — Manual switch after reconciliation:** controlled risk, longer detection/decision time and operational dependence.

#### Q29. How should backlog drain after recovery be controlled?

- **A — Rate-limited drain with reserved live-traffic capacity (Recommended baseline):** avoids a second outage, backlog clears slower.
- **B — Maximum-speed drain:** clears quickly, can overwhelm providers/state stores and starve live work.
- **C — Drop all expired/low-priority backlog then drain:** protects current traffic, only valid with approved expiry/loss semantics.

#### Q30. How is poison work isolated?

- **A — Stage-specific DLQ after bounded attempts (Recommended baseline):** clear ownership and repair context, more topics/workflows.
- **B — One global DLQ:** simple, harder triage and safe replay.
- **C — Retry forever:** no explicit loss, creates cost, blockage, and retry storms.

### F. Security, privacy, and tenancy

#### Q31. What data may be stored in Kafka?

- **A — Minimal classified data plus references (Recommended baseline):** reduces privacy risk; consumers may need authorized lookup.
- **B — Full render payload:** self-contained/replayable, increases PII footprint and deletion complexity.
- **C — Opaque encrypted blob per producer:** strong confidentiality, limits platform routing, validation, and template use.

#### Q32. How should webhook destinations be permitted?

- **A — Public HTTPS with SSRF validation and egress controls (Recommended baseline):** flexible and secure with careful implementation.
- **B — Pre-approved domain/IP allowlist only:** strongest control, high onboarding/maintenance burden and DNS issues.
- **C — Any URL supplied per event:** flexible, unacceptable SSRF/exfiltration risk.

#### Q33. What tenant isolation level is required?

- **A — Shared platform with logical isolation and quotas (Recommended baseline):** efficient; demands rigorous auth/data-key testing.
- **B — Dedicated topics/worker pools for premium tenants:** better noisy-neighbor isolation, more operational complexity.
- **C — Dedicated cluster/data plane per tenant:** strongest isolation, usually cost-prohibitive at scale.

#### Q34. How should deletion/privacy requests interact with event retention?

- **A — Minimize/tokenize data and delete queryable mappings (Recommended baseline):** Kafka log remains operationally manageable; policy/legal review required.
- **B — Encrypt per subject and destroy key (“crypto-shred”):** useful for immutable logs, complex key lifecycle and proof.
- **C — Keep all events until normal retention:** simplest, may fail legal/privacy timelines.

### G. Operations, ownership, and cost

#### Q35. What status should customers see?

- **A — Detailed normalized lifecycle (Recommended baseline):** accepted/planned/submitted/delivered-if-known/failed/etc.; transparent but requires careful semantics.
- **B — Pending/success/failure:** simple, hides provider ambiguity.
- **C — Acceptance only:** cheapest, insufficient for support and trust.

#### Q36. How are high-impact replays authorized?

- **A — Preview + scoped RBAC + approval + rate limit (Recommended baseline):** safer and auditable; slower response.
- **B — On-call engineer can run directly:** fast, high blast-radius risk.
- **C — Replays disabled:** safest from duplicates, prevents operational recovery.

#### Q37. How much observability data is retained?

- **A — Tiered by signal and compliance need (Recommended baseline):** short searchable detail, longer aggregated/audit storage.
- **B — All logs/traces for event retention:** easy investigation, high cost and PII/cardinality risk.
- **C — Metrics only:** cheap, weak per-delivery diagnosis and audit.

#### Q38. What is the preferred provider strategy per channel?

- **A — Primary provider + tested optional fallback (Recommended baseline):** controlled complexity and resilience.
- **B — One provider only:** simplest and cheapest; provider outage is accepted risk.
- **C — Load-balance every send across providers:** continuous validation, greatest dedupe/status/template/operational complexity.

#### Q39. What cost objective governs architecture?

- **A — Explicit cost per million successful notifications (Recommended baseline):** connects cost to outcome and exposes retries/failures.
- **B — Fixed monthly infrastructure cap:** predictable, may force throttling as traffic grows.
- **C — Reliability regardless of cost:** sometimes valid for critical alerts, usually too vague for the whole platform.

#### Q40. Who owns an event from acceptance through provider ambiguity?

- **A — One notification-platform team with channel/provider on-call (Recommended baseline):** clear accountability; needs domain escalation paths.
- **B — Upstream producer team:** domain-aligned but duplicates provider expertise and fragments operations.
- **C — Shared informal ownership:** flexible on paper, slow incident response and unresolved gaps.

## 12. Decision log to complete

Copy one row per questionnaire decision or ADR:

| Decision ID | Question/ADR | Selected option | Owner | Date | Rationale | Consequences | Review trigger |
|---|---|---|---|---|---|---|---|
| DEC-001 | Example: Q7 peak meaning | TBD | TBD | TBD | TBD | TBD | Traffic forecast changes by 25% |

Minimum ADR set:

1. Delivery semantics and durable acknowledgement boundary.
2. Kafka cluster and topic topology.
3. Partition key and partition sizing.
4. Dedupe identity, store, and retention.
5. Ordering scopes and failed-sequence behavior.
6. Retry scheduler/topic strategy.
7. Delivery state store and state machine.
8. Provider selection/failover and ambiguous-timeout policy.
9. Regional DR and fencing.
10. Event data classification, retention, deletion, and replay.

## 13. Validation and test plan requirements

### Contract and correctness

- Producer schema compatibility, invalid envelopes, oversized payloads, unauthorized tenants/event types.
- Same ID/same payload, same ID/different payload, concurrent duplicates, expired dedupe TTL, and replay.
- Stable delivery ID through crash/rebalance/retry; unique constraint/conditional update races.
- Sequence duplicate, gap, stale event, failed predecessor, key migration, and partition-count change.
- Provider callback duplicate, out-of-order callback, forged callback, and unknown delivery.

### Performance and resilience

- 100k events/s for the confirmed duration, representative sizes/fan-out/channel mix, plus headroom.
- Hot tenant/key and non-uniform partitions.
- Broker loss, zone loss, ISR shrink, slow disk/network, consumer crash/rebalance, and dependency latency.
- Full provider outage, partial throttling, retry storm, configuration outage, dedupe/state-store outage, and DLQ surge.
- Backlog growth and controlled drain while new live traffic continues.
- Soak test long enough to expose leaks, offset drift, disk growth, compaction/retention behavior, and provider quota cycles.

### Security and privacy

- Tenant boundary tests, privilege escalation, secret/log scanning, payload/template injection, callback forgery/replay.
- Webhook SSRF including private IPs, redirects, DNS changes, encoded IP forms, metadata endpoints, and large/slow responses.
- Retention/deletion, backup copies, audit integrity, key/secret rotation, and incident-access controls.

### Disaster recovery

- Restore state/config/dedupe databases and rebuild materialized views from defined sources.
- Kafka cluster/region failover with measured RPO/RTO, producer cutover, consumer offsets, schema/config availability, delivery fencing, and callback routing.
- Failback without dual dispatch, missing status, or duplicate storm.

## 14. Release gates / definition of ready for implementation

The HLD is ready for implementation only when:

- questionnaire decisions Q1–Q40 have accountable owners and selected answers;
- workload worksheet includes peak duration, payload percentiles, fan-out distribution, channel mix, retry factor, growth, and quotas;
- event envelope and channel contracts are versioned and reviewed by representative producers/consumers;
- SLOs, RTO/RPO, data classifications, retention, and regional rules are approved;
- Kafka topic inventory, keys, partitions, replication, retention, ACLs, schemas, and owners are documented and benchmark-backed;
- delivery state machine, dedupe TTL/authority, ordering failure behavior, retry/DLQ/replay policy, and ambiguous-provider outcome are explicit;
- dependency failure table has runbooks, alerts, test owners, and degraded modes;
- threat model covers multi-tenancy, webhooks/SSRF, callbacks, secrets, templates, PII, admin/replay, and abuse;
- capacity and cost estimates include normal, peak, one-broker/zone loss, backlog recovery, and forecast growth;
- the team agrees on phased scope and has rejected premature features explicitly.

## 15. Staff-engineer review checklist and instincts

Before signing off, ask:

- Can every “exactly once,” “ordered,” “real time,” and “highly available” statement be rewritten with a scope, number, and failure boundary?
- Can a producer distinguish rejected, durably accepted, planned, provider accepted, delivered-if-known, failed, suppressed, expired, and unknown?
- Does every retry cross an idempotency boundary, and is the identity stable long enough?
- What happens if the process dies immediately before and after each state transition or external call?
- Is any synchronous dependency on the acceptance path avoidable?
- Will one tenant, ordering key, endpoint, provider, replay, or event type starve others?
- Are partitions sized for throughput and recovery with skew, not aesthetics?
- Are we using Kafka for streaming/log semantics and a database for querying/coordination where appropriate?
- Can we increase partition count, change schema, rotate secrets, change provider, rebuild views, and roll back safely?
- Are retry and DLQ mechanisms compatible with ordering promises?
- Does DR prevent two regions from sending the same notification?
- Can on-call see oldest work age and drain time, and perform a safe bounded repair?
- Do retention and observability choices satisfy deletion/privacy and cost limits?
- Has the team tested the uncomfortable cases: unknown provider outcome, lost callback, corrupt config, restored old state, and partial failover?
- Is complexity justified by a requirement, benchmark, risk, or cost calculation?

The core architectural instinct is to make failure semantics explicit. High throughput is usually achievable; trustworthy behavior during retries, partial failures, replays, and operator intervention is what distinguishes a production notification platform from a demo.
