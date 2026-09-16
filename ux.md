# Operator Console UX Specification

**Status:** Planned and ready for discovery/prototyping  
**Scope:** Event Notification Service operator and tenant-administration experience

## 1. UX outcome

An operator should be able to answer these questions quickly and safely:

1. Was the event durably accepted?
2. What happened to every intended notification?
3. Is processing delayed, failed, suppressed, expired, duplicated, or unknown?
4. Is the problem one event, tenant, endpoint, provider, channel, partition, or the whole platform?
5. What can I safely do next, and what impact will that action have?
6. Will my action create duplicate user-visible notifications?

The UX must teach the system's semantics rather than hide them. In particular, `accepted`, `materialized`, `provider accepted`, `delivered if known`, and `unknown outcome` must remain distinct.

## 2. Primary personas

### Support investigator

- Starts with an event ID, order/account ID, recipient reference, or approximate time.
- Needs a plain-language timeline and a shareable investigation link.
- Must not see sensitive payload fields outside their permission.
- Usually cannot replay or modify configuration.

### Tenant operator

- Manages endpoints, subscriptions, templates, and tenant-specific health.
- Needs validation before activation and protection from cross-tenant mistakes.
- Understands their domain but not Kafka internals.

### Platform on-call engineer

- Starts with an alert, lag spike, provider outage, hot partition, retry storm, or DLQ growth.
- Needs scope, oldest age, trend, impact, recent change, and safe mitigation.
- Uses deeper observability tools but needs the console for supported operations.

### Security/audit reviewer

- Investigates identity conflicts, secret rotation, unusual replay, privilege changes, and webhook destinations.
- Needs immutable evidence, actor/reason/correlation, and export under policy.

### Approver

- Reviews a prepared high-impact action.
- Needs a stable snapshot of scope and predicted impact, not merely an “Approve” button.

## 3. Experience principles

1. **Lead with impact, then mechanism.** Say “2,431 critical notifications are delayed” before “consumer lag is 18,204.”
2. **Show age as well as count.** Oldest item age often expresses user harm better than queue size.
3. **Never collapse unknown into failed or successful.** Unknown provider outcomes require their own state and guidance.
4. **Make scope persistent.** Tenant, region, environment, channel, time range, and filters remain visible.
5. **Progressive disclosure.** Plain-language summary first; Kafka/provider/config details one level deeper.
6. **Prevent unsafe speed.** High-impact operations use preview, reason, permission, thresholds, and audit.
7. **Preserve investigation context.** Filters and selected tabs live in shareable, non-sensitive URLs.
8. **Prefer repair workflows to raw mutation.** Operators invoke bounded commands; they do not edit database fields.
9. **Design failure states first.** Loading and happy-path mockups are insufficient for an operations product.
10. **Be honest about freshness.** Every derived view communicates its time window and last update.

## 4. Status language

Use consistent user-facing language:

| Machine concept | User label | Meaning |
|---|---|---|
| `ACCEPTED` | Accepted | Kafka durably acknowledged the event under configured durability. |
| `MATERIALIZED` | Recorded | The query/dedupe view processed the accepted event. |
| `PLANNED` | Planned | A logical delivery intent exists. |
| `SUPPRESSED` | Suppressed | Policy/preferences intentionally prevented delivery. |
| `ATTEMPTING` | Sending | A provider/endpoint attempt is active. |
| `RETRY_WAIT` | Waiting to retry | A transient failure has a scheduled next attempt. |
| `PROVIDER_ACCEPTED` | Submitted | Provider accepted responsibility; human/device receipt is not implied. |
| `DELIVERED` | Delivered (reported) | Provider callback reported delivery where supported. |
| `UNKNOWN` | Outcome unknown | Request may have succeeded, but confirmation was lost/unavailable. |
| `FAILED` | Permanently failed | Policy has no more automatic attempts. |
| `EXPIRED` | Expired | Work became too old to send. |
| `DLQ` | Needs review | Automated processing quarantined the record. |

Never use a generic green “Success” for provider acceptance.

## 5. Key journeys

### Journey A: investigate one event

1. Paste/search event ID.
2. See identity and current summary immediately.
3. Read a chronological timeline: occurred → accepted → materialized → planned → attempts → outcome.
4. Inspect dedupe count/conflict, ordering key/sequence, expiry, configuration versions, and safe payload.
5. Follow links to each delivery or related trace/log tool.
6. Copy a sanitized investigation link/correlation ID.

Success metric: a trained support user determines the current state and owner of the next action in under two minutes.

### Journey B: understand a backlog alert

1. Arrive from an alert deep link with environment/channel/provider/time scope.
2. See impacted tenants/priorities, oldest age, arrival rate, processing rate, and estimated drain time.
3. Compare with recent deploy/config/provider change markers.
4. Drill into representative failures without sampling only the newest records.
5. Follow the runbook or invoke a permitted bounded action.

Success metric: on-call identifies blast radius and whether backlog is growing or draining without querying the database.

### Journey C: configure and verify a webhook endpoint

1. Enter public HTTPS URL and metadata.
2. UI explains verification and network/security constraints before saving.
3. Backend validates, stores securely, and performs challenge/verification.
4. Secret appears once; operator acknowledges secure storage.
5. Endpoint remains inactive until verified.
6. Health/history view explains failures and rotation status.

Success metric: endpoint cannot accidentally become active without verification, and the secret cannot later be revealed.

### Journey D: replay quarantined work

1. Select a narrow saved filter or specific items.
2. Request preview.
3. Review exact count, tenants, channels, ages, expiry, dedupe behavior, estimated rate/duration, and duplicate-risk warning.
4. Enter reason; obtain approval if threshold requires it.
5. Execute asynchronously with immutable scope snapshot/idempotency key.
6. Monitor processed/skipped/failed/stopped counts.
7. View resulting audit record and follow-up failures.

Success metric: no replay can start from a stale or unbounded selection, and repeated submission does not create a second job.

### Journey E: edit and activate a template

1. Fork active version into draft.
2. Edit by channel/locale and validate required variables.
3. Preview only with sanitized sample data.
4. Compare diff with active version.
5. Activate using optimistic version check and record reason.
6. Roll back by activating a prior immutable version, not mutating history.

Success metric: an operator knows exactly which version new delivery intents will use and cannot overwrite another edit silently.

## 6. Low-fidelity page frames

### Event list

```text
+ Event Notification Service ---------------- Tenant [Acme] Environment [Prod] +
| Events | Deliveries | Endpoints | Operations | Audit                         |
+-------------------------------------------------------------------------------+
| Events                                  Updated 10:42:18 UTC  [Refresh]        |
| [Event ID........] [Type......] [Status...] [From...] [To...] [Apply]         |
| Active filters: tenant=Acme · last 24h                      [Clear all]        |
+-------------------------------------------------------------------------------+
| Received UTC | Event ID | Type | Source | State | Seq | Duplicate/conflict    |
| ...                                                                           |
+-------------------------------------------------------------------------------+
| 50 rows | Previous | Next cursor                                               |
+-------------------------------------------------------------------------------+
```

### Event detail

```text
+ Event event-0001 -------------------------------------------------------------+
| Recorded | order.shipped | tenant Acme | received 10:41:02 UTC                |
| WARNING: identity conflict detected (only when applicable)                    |
+-------------------------------------------------------------------------------+
| [Timeline] [Payload] [Ordering] [Deliveries] [Technical]                      |
| 10:40:59 Occurred                                                            |
| 10:41:01 Accepted by event backbone  topic/partition/offset                   |
| 10:41:02 Recorded; duplicate count 0                                          |
| ...                                                                           |
+-------------------------------------------------------------------------------+
```

### Replay preview

```text
+ Replay preview ---------------------------------------------------------------+
| Immutable scope: webhook DLQ · tenant Acme · 10:00–10:30 UTC                  |
| Eligible 1,204 | Expired 88 | Already completed 12 | Unknown-risk 3          |
| Estimated 4m at 5/s; live traffic retains reserved capacity                   |
| Duplicate policy: stable delivery IDs; 3 ambiguous outcomes need approval     |
| Reason [.................................................................]      |
| [Cancel]                                            [Request approval]         |
+-------------------------------------------------------------------------------+
```

## 7. Error and recovery language

Errors must answer:

- what happened;
- whether the requested action may have occurred;
- what the user can safely do;
- whether retry uses the same idempotency key;
- correlation ID and support/runbook link.

Examples:

- **Safe retry:** “The event was not accepted because Kafka was unavailable. Retry with the same event ID.”
- **Unknown command outcome:** “We lost confirmation after submitting the replay request. Do not create another replay. Check job key `…` or refresh.”
- **Stale edit:** “This subscription changed after you opened it. Compare the current version before applying your changes.”
- **Forbidden:** “You can view this replay, but only a platform approver can authorize more than 1,000 deliveries.”

Avoid blameful or internal-only messages such as stack traces, SQL errors, or “consumer exception.”

## 8. Research and validation plan

### Discovery

- Interview at least one support user, platform on-call, tenant admin, and security/audit stakeholder.
- Observe current incident/search/replay workflows and tools.
- Collect top investigation starting points and common mistaken assumptions.
- Validate terminology: event, notification, delivery, attempt, submitted, delivered, suppressed, expired, DLQ.

### Prototype tests

Test these scenarios:

1. find why one notification was not received;
2. distinguish provider accepted from delivered;
3. identify whether a backlog is improving;
4. configure and verify a webhook endpoint;
5. review a conflicting event ID;
6. preview a replay and explain its duplicate risk;
7. recover from a stale edit/permission failure.

Measure task completion, time, critical errors, terminology misunderstanding, and confidence. Do not measure success only by whether users “like” the screens.

## 9. UX work packets for AI agents

AI-generated UX outputs are hypotheses until reviewed by a real operator/domain owner.

| ID | Work packet | Inputs | Output | Suggested budget | Acceptance |
|---|---|---|---|---:|---|
| UX-001 | Persona/interview guide and assumption map | `overview.md`, `requirements.md` | `docs/ux/research-plan.md` | 6k | Questions target real decisions; no invented research findings. |
| UX-002 | Information architecture and navigation validation | UI/UX specs | `docs/ux/information-architecture.md` | 8k | All tasks map to a route; role restrictions documented. |
| UX-003 | Event investigation wireframes/states | Phase 1 API, status taxonomy | `docs/ux/event-investigation.md` | 10k | Happy/failure/stale/forbidden/empty paths covered. |
| UX-004 | DLQ/replay safety journey | Retry/replay requirements | `docs/ux/replay-workflow.md` | 10k | Preview, ambiguity, approval, idempotency, stop, audit represented. |
| UX-005 | Endpoint/template administration flows | Phase 2/3 contracts | `docs/ux/control-plane-flows.md` | 12k | Verification/versioning/secret/unsafe-content paths covered. |
| UX-006 | Usability test protocol and synthesis template | clickable prototype | `docs/ux/usability-test.md` | 7k | Tasks, measures, consent/redaction, finding severity defined. |
| UX-007 | Content/status/error language catalog | backend error codes | `docs/ux/content-guidelines.md` | 8k | One meaning per status; safe retry/unknown outcome explicit. |

Agents must label assumptions and unresolved questions. They must not claim user validation without actual sessions/evidence.

## 10. UX acceptance checklist

- The current tenant/environment/time scope is always visible.
- Status wording matches backend semantics.
- User impact is shown before infrastructure mechanism.
- Unknown outcomes have a safe next step.
- Every privileged action previews scope and consequence.
- No sensitive data appears unnecessarily.
- Journeys include partial failure, stale data, permission, and concurrency states.
- Keyboard/screen-reader behavior is part of the design, not deferred to QA.
- Operator testing validates critical workflows before production enablement.

