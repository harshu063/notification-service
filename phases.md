# Implementation Phases

Each phase delivers a demonstrable vertical capability and a set of HLD lessons. A phase is complete only when its acceptance tests and operational evidence pass.

## Phase 0 — Decisions and contracts

**Goal:** remove ambiguity before costly implementation.

Deliverables:

- answer the priority questions in `requirements.md`, especially peak duration, fan-out, SLO, ordering scope, retry ambiguity, retention, RTO/RPO, and tenancy;
- approve canonical event schema and compatibility policy;
- record ADRs for acknowledgement, Kafka topology, identity/dedupe, ordering, retries, state store, and region model;
- define initial service-level indicators and load model.

Learning focus: requirement discovery, consistency boundaries, capacity math, and explicit trade-offs.

Exit gate: stakeholders can explain what `202 Accepted`, `provider accepted`, `delivered`, `failed`, and `unknown` each mean.

## Phase 1 — Durable ingestion and materialization (planned first build phase)

**Goal:** create the first end-to-end Kafka path.

Planned deliverables:

- Spring Boot application and Docker build;
- validated HTTP ingestion API;
- Kafka producer with stable record key and durable acknowledgement settings;
- canonical accepted-event topic declaration;
- idempotent Kafka consumer group;
- PostgreSQL event materialization via JPA;
- Flyway schema migration;
- event status query;
- duplicate and conflicting event-ID behavior;
- health, metrics, Compose environment, and unit tests.

Required validation to complete Phase 1:

- integration tests with real Kafka/PostgreSQL through Testcontainers or the Compose stack;
- schema registry and compatibility CI;
- authentication/tenant authorization suitable for a non-local environment;
- load generator and baseline benchmark;
- consumer error/DLQ policy and dashboards;
- graceful shutdown/rebalance failure tests.

Learning focus: producers, brokers, topics, partitions, keys, offsets, consumer groups, at-least-once delivery, and eventual consistency.

Exit gate: crash/retry/rebalance tests show no duplicate materialized event and no acknowledged event loss within the agreed local test boundary.

### Parallel UI/UX/QA stream after the Phase 1 API stabilizes

- UX validates the operator information architecture and event-detail journey.
- UI builds the console shell, API client, event search/detail pages, and shared loading/error/empty states.
- QA creates contract, accessibility, component, and end-to-end coverage for the same read-only journey.
- Work is divided using `agent-workstreams.md`; detailed specifications live in `ui.md`, `ux.md`, and `qa.md`.

## Phase 2 — Webhook vertical slice

**Goal:** deliver one channel correctly before generalizing.

Deliverables:

- tenant/subscription/verified-endpoint model and APIs;
- routing consumer that produces deterministic webhook delivery intents;
- delivery state machine with unique `deliveryId` and attempt history;
- signed requests, timestamp/replay protection, connect/read/overall timeouts;
- strict SSRF, redirect, DNS, IP, response-size, and egress controls;
- per-endpoint/provider/tenant concurrency and rate limits;
- retry with exponential backoff/full jitter, expiry, DLQ, and replay preview;
- test webhook fixture supporting delay, timeout, disconnect, 2xx, 4xx, 429, and 5xx.

Learning focus: side-effect ambiguity, idempotency keys, circuit breakers, bulkheads, backpressure, retry storms, and operational repair.

Exit gate: fault injection demonstrates bounded resources, isolated slow endpoints, correct retry classification, signed callbacks, and safe replay.

UI addition: subscription/endpoint administration, delivery timeline, and a disabled-by-default replay workflow with preview and explicit authorization.

## Phase 3 — Templates, preferences, email, and mobile push

**Goal:** turn the webhook pipeline into a multi-channel notification platform.

Deliverables:

- versioned templates by tenant/event/channel/locale;
- deterministic rendering and contextual escaping;
- preference, consent, quiet-hours, locale, suppression, expiry, and priority policy;
- email provider adapter and authenticated delivery/bounce/complaint callbacks;
- APNs/FCM adapters, TTL/collapse/priority, token hygiene, and client telemetry semantics;
- separate Kafka intent topics and consumer groups per channel;
- provider abstraction that normalizes outcomes without hiding provider details;
- channel-level quotas, cost, latency, success, and deliverability dashboards.

Learning focus: control plane versus data plane, versioned configuration, adapter boundaries, provider semantics, privacy, and channel isolation.

Exit gate: a canonical event fans out through selected preferences to all three channels with traceable terminal state and no duplicate delivery in supported idempotent cases.

UI addition: template lifecycle, channel configuration, preference-policy inspection, provider health, and normalized status views.

## Phase 4 — Strict ordering, retry scheduling, and replay

**Goal:** make exceptional sequencing behavior explicit and testable.

Deliverables:

- event-class ordering configuration;
- per-key source sequence state, duplicate/stale/gap detection, and fenced ownership;
- per-key hold/reconcile/skip/DLQ policy for strict streams;
- scalable retry scheduler or hybrid retry-topic strategy;
- replay with original identity, dry-run count, RBAC/approval, rate limit, audit, and live-traffic reservation;
- partition expansion/migration procedure.

Learning focus: partition order versus business order, head-of-line blocking, stale state, fencing, and replay safety.

Exit gate: sequence gaps, failed predecessors, crashes, rebalances, replay, and partition-count change behave according to documented policy.

## Phase 5 — Scale to the agreed 100k-events/s profile

**Goal:** prove—not assume—the workload target.

Deliverables:

- representative load generator with payload/fan-out/channel/skew/retry distributions;
- measured per-partition producer and consumer capacity;
- production topic partition count and broker sizing with N+1/AZ-loss headroom;
- JPA/JDBC/table/index/partition optimization driven by profiles;
- autoscaling based on ingress, lag, oldest age, processing rate, and provider quota;
- rate-limited backlog drain while live traffic continues;
- performance/cost report at normal, peak, failure, and recovery load.

Learning focus: throughput versus latency, Little's Law, hot keys, queue age, saturation, broker sizing, and cost per successful notification.

Exit gate: the confirmed 100k profile passes SLOs for the required duration with headroom and one approved failure scenario.

## Phase 6 — Security, multi-tenancy, and production readiness

**Goal:** safely operate the service for real tenants.

Deliverables:

- workload/human authentication and tenant/role/event/channel authorization;
- Kafka ACLs/quotas, encryption, secret rotation, and audit;
- PII classification/minimization, retention/deletion, residency, legal hold, and subject-request procedures;
- WAF/abuse protection, template/content safety, dependency/container security;
- SLO/error-budget dashboards, alerts, runbooks, ownership, incident exercises;
- canary/rolling deployment, expand-contract migrations, rollback, and dependency compatibility policy.

Learning focus: threat modeling, noisy-neighbor controls, secure operations, error budgets, and socio-technical ownership.

Exit gate: security review, operational readiness review, chaos game day, and on-call handoff are complete.

UI addition: production identity/RBAC, tenant scoping, immutable audit visibility, accessible keyboard workflows, and redaction of sensitive payload fields.


## Phase 7 — Regional disaster recovery

**Goal:** meet the approved regional RTO/RPO without duplicate dispatch.

Deliverables:

- replicated/restorable Kafka data, schemas, PostgreSQL state, configuration, templates, secrets, and callback routing;
- regional ownership/fencing and producer cutover;
- offset/checkpoint, dedupe identity, failover, and failback procedures;
- backup/restore and full-region game days with measured results;
- decision on whether warm standby is sufficient or active-active is justified.

Learning focus: RPO/RTO, split brain, cross-region order, fencing, failback, and consistency of an entire system rather than one datastore.

Exit gate: a full test meets RTO/RPO and reconciliation finds no unexplained missing or duplicate logical deliveries.

## Recommended learning cadence for every phase

1. Write or update the requirement and ADR.
2. Draw the event/state/failure path.
3. Implement the smallest vertical behavior.
4. Test the happy path and the crash immediately before/after each durable boundary.
5. Add metrics, alert, repair operation, and runbook.
6. Load/fault test the phase.
7. Review what complexity is now justified and what remains deferred.
