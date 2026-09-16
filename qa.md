# Quality Assurance Strategy

**Status:** Active from Phase 1  
**Scope:** Backend, Kafka/PostgreSQL data path, operator console, providers, security, performance, and disaster recovery

## 1. Quality model

The most important quality claim is not “the API returns 200.” It is:

> An accepted event reaches an explainable, policy-correct state under retries, crashes, rebalances, provider ambiguity, backlog, replay, and operator intervention without violating tenant isolation.

QA is a continuous engineering stream. Test plans, fixtures, observability, failure injection, and repair verification are implemented with each phase.

## 2. Quality risks ordered by impact

1. An acknowledged event is lost.
2. A logical notification is sent more than intended.
3. Tenant data or actions cross tenant boundaries.
4. Ordering policy produces stale or contradictory user-visible effects.
5. Retry/backlog recovery overwhelms providers or live traffic.
6. A timeout is reported as definite failure/success when outcome is unknown.
7. Replay bypasses consent, expiry, dedupe, permissions, or audit.
8. Webhooks enable SSRF, secret leakage, unsafe redirects, or forged callbacks.
9. Schema/config/deployment changes break in-flight records.
10. Status/observability is stale or misleading during incidents.
11. UI hides critical state or enables a dangerous accidental action.
12. System meets average load but fails the 100k peak profile, skew, or recovery case.

## 3. Test layers

### Static and build checks

- Java compilation, formatting/style policy, dependency analysis, secret scan, SAST, container/IaC scan;
- migration validation and schema compatibility;
- TypeScript strict compilation, lint, dependency and bundle checks;
- API/schema generated-client drift check;
- architecture tests preventing forbidden module dependencies when added.

### Unit tests

Backend:

- validation and business time rules;
- canonical payload hashing;
- stable event/delivery ID derivation;
- duplicate/conflict classification;
- retry/error classification;
- ordering/gap state transitions;
- policy/preference/template selection.

UI:

- status/error mapping;
- URL filter encoding;
- permission-aware visibility (in addition to backend auth tests);
- formatters and redaction-safe display;
- forms, validation, confirmations, and state reducers.

### Component/slice tests

- Spring MVC API error/response contracts without external systems;
- JPA repository behavior against PostgreSQL, not an incompatible in-memory database;
- Kafka serialization/deserialization and listener error handling;
- UI features against a contract-faithful mock server;
- accessibility automation for rendered pages/components.

### Integration tests

- real Kafka + PostgreSQL using Testcontainers or Compose in CI;
- producer acknowledgement and consumer offset/database transaction behavior;
- duplicate records, crash/restart, consumer rebalance, poison records, and DLQ;
- migrations from the previous supported schema;
- webhook fixture/provider sandbox, callback authentication, and outcome normalization;
- frontend against a deployed backend test environment.

### End-to-end tests

- submit event → Kafka → materialize → query;
- event → route → channel intent → provider fixture → status timeline;
- subscription/template/endpoint lifecycle;
- DLQ preview → approval → replay job → resulting deliveries/audit;
- authentication, tenant switching/scope, session expiration, and forbidden actions;
- UI loading/empty/error/stale/unknown outcome and browser back/forward/deep links.

### Performance and resilience tests

- confirmed 100k-events/s profile with representative sizes and fan-out;
- p50/p95/p99 API and end-to-end latency;
- hot ordering keys and tenant skew;
- retries and provider throttling;
- broker/consumer/database loss and recovery;
- sustained soak, retention/compaction, disk growth, connection pools, and memory;
- backlog drain while new live traffic retains reserved capacity;
- UI query behavior on high-cardinality datasets and slow/partial API responses.

### Security and privacy tests

- authentication/authorization and cross-tenant object access;
- webhook SSRF variants, redirect chains, DNS rebinding, private/link-local/metadata addresses, slow/large responses;
- signature/callback replay/forgery and secret rotation overlap;
- XSS/template injection, CSRF/session policy, CSP, unsafe links, dependency risks;
- PII in logs, metrics, traces, URLs, browser storage, exports, DLQ, backups, and screenshots;
- replay/approval/audit tampering and privilege escalation;
- deletion/retention behavior across materialized views and backups per policy.

### Disaster recovery tests

- database restore and materialized-view rebuild;
- Kafka failover with schema/config/state/secrets/callback dependencies;
- regional fencing: never dispatch actively from two owners unintentionally;
- measured RPO/RTO and reconciliation of missing/duplicate/unknown deliveries;
- failback under live traffic.

## 4. Phase 1 test matrix

| ID | Scenario | Expected result | Level | Current state |
|---|---|---|---|---|
| P1-001 | Valid event | `202` after Kafka ack; receipt includes event/topic/partition/offset | Integration | To add |
| P1-002 | Invalid envelope/type | `400` with stable code and field errors; no Kafka record | API | To add |
| P1-003 | Expired event | `400 INVALID_EVENT`; no Kafka record | Unit/API | Planned |
| P1-004 | Kafka unavailable | `503`; producer instructed to retry same event ID | Integration | To add |
| P1-005 | Event materialization | One PostgreSQL row after consumer commit | Integration | Planned |
| P1-006 | Same ID/same content retry | One row; duplicate count increments | Integration | Planned |
| P1-007 | Same ID/different content | Existing event retained; conflict flag/audit signal | Integration | Planned |
| P1-008 | Consumer crash before DB commit | Record redelivered; final one logical row | Failure integration | To add |
| P1-009 | Consumer crash after DB commit/before offset commit | Duplicate redelivery becomes no-op | Failure integration | To add |
| P1-010 | Consumer rebalance | No unexplained loss; bounded duplicate behavior | Resilience | To add |
| P1-011 | Nested JSON field order differs | Same canonical content hash | Unit | Planned |
| P1-012 | Status immediately after acceptance | May be `404`, later `200`; documented eventual consistency | E2E | To add |
| P1-013 | Database unavailable | Kafka backlog grows; consumer does not commit processed offset | Integration | To add |
| P1-014 | Topic has expected partitions/config | 12/1 locally; environment overrides validated | Integration | To add |
| P1-015 | Graceful shutdown | In-flight handler completes/rolls back; clean rebalance | Resilience | To add |

## 5. API contract QA

Every endpoint must test:

- documented success status/body/headers;
- validation boundaries and multiple field failures;
- authentication, forbidden tenant/object, and non-existent object;
- idempotency and duplicate command submission;
- optimistic concurrency conflict where state is edited;
- pagination stability under concurrent inserts;
- maximum page/payload/filter/time-range limits;
- rate limiting and retry guidance;
- dependency timeout and unknown outcome;
- machine-readable error code, safe message, and correlation ID;
- backward compatibility with the previous supported client/schema.

Maintain API examples or an OpenAPI contract as executable test inputs. A UI agent must not independently invent fields.

## 6. Kafka-specific QA

Test with real broker behavior:

- correct record key for the promised ordering scope;
- partition distribution and hot keys;
- `acks=all`, idempotent producer, bounded delivery timeout;
- serialization compatibility across old/new producer and consumer versions;
- offset commit only after durable result;
- redelivery after termination at each boundary;
- cooperative rebalance/deployment behavior;
- poison record bounded retry and stage-specific DLQ;
- retry-topic/per-key scheduler interaction with ordering;
- retention/replay window and dedupe TTL compatibility;
- broker/leader loss, ISR shrink, and expected rejection versus durability behavior;
- partition-count change procedure and sequence correctness.

Never claim exactly-once external notification delivery based on Kafka transaction tests.

## 7. Provider/webhook test fixture

Build a controllable fixture that can respond per delivery ID with:

- immediate 2xx;
- delayed 2xx before/after timeout;
- connection reset before/after body;
- 400/401/403/404/409/410/422;
- 429 with valid/invalid/missing `Retry-After`;
- 500/502/503/504;
- redirect to allowed/disallowed/private targets;
- slow headers/body and oversized response;
- duplicate/out-of-order callbacks;
- accepted submission with lost response to create ambiguous outcome.

The fixture must record request time, headers, body hash, delivery ID, attempt number, and concurrency without storing production secrets/PII.

## 8. UI QA

For every UI route:

- smoke render and route/deep-link test;
- loading, empty, background refresh, stale, partial, error, rate-limited, forbidden, and unauthenticated states;
- keyboard navigation, focus order/restoration, visible focus, accessible name/description, landmarks/headings;
- automated accessibility scan plus manual keyboard/screen-reader spot check;
- responsive layout at agreed viewports/zoom;
- timestamps/timezone and large-number formatting;
- no status by color alone;
- safe rendering of hostile payload/error/template strings;
- no PII/secrets in URL, logs, analytics, browser storage, snapshot artifacts;
- command double-submit/idempotency, unknown response, stale version, and confirmation flow;
- visual regression only for stable core components/pages; do not use screenshots as the only assertion.

Critical browser journeys:

1. exact event investigation;
2. filtered event list with shareable URL;
3. endpoint verification/rotation;
4. template draft/diff/activate/rollback;
5. replay preview/approval/execution/stop;
6. expired session during a command;
7. cross-tenant URL manipulation is rejected by the server.

## 9. Test environments and data

| Environment | Purpose | Data policy |
|---|---|---|
| Unit/local | Fast logic and component feedback | Generated synthetic data only. |
| Compose | Developer integration with Kafka/PostgreSQL/app | Resettable synthetic volumes. |
| CI ephemeral | Repeatable per-change integration/browser tests | Unique synthetic tenant/run IDs; automatic cleanup. |
| Performance | Isolated representative scale and fault tests | Synthetic distributions; no production PII. |
| Staging | Release candidate, provider sandbox, migration/recovery rehearsal | Approved masked/synthetic data. |
| Production | Synthetic probes and carefully scoped verification | No destructive test; explicit approvals/runbooks. |

Test data builders must support event type, tenant, ordering key, sequence, payload size, fan-out, priority, expiry, duplicate/conflict, and sensitive-field markers. Time-dependent tests use an injected clock.

## 10. Defect severity and release policy

- **S0:** confirmed tenant/security breach, destructive replay, accepted-event loss across promised boundary, uncontrolled duplicate critical notification. Stop/revoke/rollback immediately.
- **S1:** major delivery outage, ordering correctness failure, inaccessible critical operator workflow, recovery unable to meet target. Release blocker.
- **S2:** partial function/SLO issue with workaround and bounded impact. Requires owner and approved disposition.
- **S3:** minor usability/diagnostic issue. May defer with backlog owner.

No release proceeds with unresolved S0/S1. Flaky tests are defects: quarantine must have owner, reason, replacement signal, and removal date.

## 11. Quality gates by phase

### Pull request

- compile/lint/unit/component tests;
- changed contract/migration compatibility;
- security/static checks;
- focused documentation/ADR updated;
- no unreviewed snapshot/golden change.

### Merge/deploy to staging

- Kafka/PostgreSQL integration tests;
- supported migration path;
- UI/browser critical read journey;
- container/image scan;
- observable version and synthetic health.

### Production release

- phase exit criteria satisfied;
- capacity/failure/security evidence appropriate to change risk;
- dashboards/alerts/runbooks/rollback ready;
- canary/gradual exposure and stop conditions;
- audit/approvals for migrations or high-impact configuration.

## 12. QA work packets for AI agents

| ID | Work packet | Depends on | Exclusive paths | Suggested budget | Acceptance |
|---|---|---|---|---:|---|
| QA-001 | Phase 1 unit/API gaps and coverage report | Current backend | `src/test/**` excluding integration package | 10k | Deterministic tests pass; no context-wide fake Kafka claims. |
| QA-002 | Kafka/PostgreSQL Testcontainers suite | Stable Phase 1 app | `src/test/**/integration/**`, test config | 16k | P1-001/004–010/012–015 automated where feasible. |
| QA-003 | API contract/OpenAPI verification | API contract | `src/test/**/contract/**`, contract files | 10k | Examples and compatibility checks run in CI. |
| QA-004 | Webhook fault fixture | Phase 2 contract | `qa/webhook-fixture/**` | 14k | Configurable timeout/status/redirect/callback/ambiguity behavior. |
| QA-005 | UI component/a11y harness | UI-001/002 | `ui/src/test/**`, test configuration | 10k | Automated scans and keyboard-focused helpers run in CI. |
| QA-006 | Browser critical journeys | UI features + deployed API | `ui/e2e/**` | 16k | Event investigation plus relevant control workflows pass. |
| QA-007 | 100k workload generator and test model | Capacity worksheet complete | `qa/load/**` | 20k | Configurable size/fan-out/skew/retry; reproducible report. |
| QA-008 | Resilience/chaos scenarios | Stable integration env | `qa/resilience/**` | 18k | Broker/consumer/DB/provider failure evidence and cleanup. |
| QA-009 | Security test plan/automation | Threat model and auth | `qa/security/**` | 16k | Tenant/SSRF/signature/XSS/secret cases mapped to controls. |
| QA-010 | DR exercise and reconciliation tooling | Phase 7 environment | `qa/dr/**`, runbook docs | 20k | RPO/RTO and missing/duplicate/unknown results measured. |

An agent completing a QA packet must report what was actually executed, what was only authored, environmental limitations, and exact remaining risks. Test code is not evidence until it has run.

## 13. QA evidence template

For each quality gate record:

```text
Build/version:
Environment:
Requirement/risk IDs:
Scenarios executed:
Data/workload profile:
Expected result:
Actual result:
Artifacts (logs/metrics/report):
Defects/limitations:
Reviewer/date:
```

## 14. Definition of done for a work packet

- Requirement and risk IDs are linked.
- Positive, boundary, failure, permission, and idempotency paths are covered as applicable.
- Tests are deterministic, isolated, and runnable by another engineer/agent.
- Assertions verify outcomes and durable state, not only HTTP status/log text.
- Sensitive data is absent from fixtures/artifacts.
- CI/local command and prerequisites are documented.
- Test run evidence is reported honestly.
- Known gaps have owners and do not contradict the claimed guarantee.
