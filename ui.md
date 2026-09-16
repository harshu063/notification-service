# Operator Console UI Specification

**Status:** Planned; implementation starts after the Phase 1 read APIs stabilize  
**Product:** Event Notification Service  
**UI type:** Internal operations and tenant-administration console

## 1. Are we creating a UI?

Yes. We are creating an **operator console**, because a notification platform cannot be operated safely through database queries and Kafka command-line tools alone.

The UI is not initially:

- an end-user notification inbox;
- a marketing campaign builder;
- a public analytics portal;
- a replacement for Grafana or the Kafka administration platform;
- a way to edit arbitrary production data.

Its purpose is to make supported workflows safe, understandable, tenant-scoped, and auditable.

## 2. Recommended frontend architecture

Use a separate TypeScript frontend application under `ui/` when implementation begins.

Recommended stack:

- React with TypeScript;
- Vite for build/development unless the hosting platform later requires server rendering;
- a query/cache library for server state;
- a small accessible component system with design tokens;
- a schema-generated or contract-tested API client;
- a router with route-level permissions;
- component tests plus browser end-to-end tests.

Do not pin framework versions in this planning document. Select current supported releases at implementation time and record them in the UI package manifest/ADR.

The console should be a stateless client of backend APIs. It must not connect directly to Kafka, PostgreSQL, email providers, APNs/FCM, or webhook endpoints.

```text
Browser
  |
  | HTTPS + authenticated session/token
  v
Operator Console SPA
  |
  | versioned REST API
  v
Notification Control/Query API
  |-- PostgreSQL read models
  |-- audited command handlers
  +-- metrics links/aggregations
```

## 3. Users and permissions

| Role | Primary capability | Explicit restriction |
|---|---|---|
| Viewer/support | Search events and deliveries, inspect sanitized diagnostics | Cannot mutate configuration or replay. |
| Tenant operator | Manage the assigned tenant's endpoints, subscriptions, and templates | Cannot view other tenants or platform internals. |
| Platform operator | Inspect system-wide health, pause/resume scoped processing, repair DLQ work | High-impact actions require reason and sometimes approval. |
| Security/auditor | Read immutable audit trails, conflict events, credential metadata | Cannot reveal secret values or execute delivery. |
| Approver | Approve bounded replay/high-impact changes | Cannot silently alter the prepared scope. |

The backend is the authorization authority. Hiding a button is not authorization.

## 4. Information architecture

### Primary navigation

1. **Overview** — service health and user-impact summary.
2. **Events** — canonical accepted-event search and detail.
3. **Deliveries** — delivery-intent/attempt search and timelines.
4. **Endpoints** — webhook endpoint verification and health.
5. **Subscriptions** — event routing configuration.
6. **Templates** — versioned content lifecycle.
7. **Operations** — DLQ, replay, pause/resume, backlog, and drain state.
8. **System** — provider/channel/Kafka dependency summaries and links to detailed observability.
9. **Audit** — configuration and privileged-action history.

Only pages supported by implemented backend behavior should be enabled. Future routes may appear as clearly labelled unavailable roadmap entries in development, but not as fake production controls.

## 5. Route plan

| Route | Phase | Purpose |
|---|---:|---|
| `/` | UI-1 | Overview with SLO/backlog/channel summaries. |
| `/events` | UI-1 | Search/filter accepted events. |
| `/events/:eventId` | UI-1 | Event envelope, status, dedupe/conflict, and later delivery fan-out. |
| `/deliveries` | UI-2 | Search logical deliveries and terminal/non-terminal states. |
| `/deliveries/:deliveryId` | UI-2 | Attempt timeline, provider outcomes, retry/expiry decisions. |
| `/endpoints` | UI-2 | Webhook endpoints and health. |
| `/endpoints/:endpointId` | UI-2 | Verification, secret-rotation metadata, rate/failure history. |
| `/subscriptions` | UI-2 | Routing subscriptions and versions. |
| `/templates` | UI-3 | Template versions, locale/channel variants, validation. |
| `/operations/dlq` | UI-2 | Quarantined work and repair eligibility. |
| `/operations/replays` | UI-2 | Preview, approve, execute, observe, and stop replay jobs. |
| `/system` | UI-3 | Dependency and provider summaries. |
| `/audit` | UI-3 | Privileged action/configuration audit. |

## 6. Phase UI-1: read-only event explorer

This is the first UI vertical slice.

### Page: Overview

Show:

- acceptance rate and error ratio;
- oldest unprocessed event age, not only record count;
- materialization lag;
- conflict/deduplication count;
- delivery summaries only when delivery data exists;
- clear data time range, last refreshed time, and partial-data warnings;
- links to the corresponding filtered event view.

Do not derive operational health from a single green/red badge. Distinguish healthy, degraded, unavailable, stale, and unknown.

### Page: Event list

Filters:

- tenant (permission scoped);
- event ID exact match;
- event type;
- source;
- ordering key;
- status;
- conflict flag;
- occurred/received time range.

Behavior:

- server-side pagination with a stable cursor rather than loading all rows;
- filter state encoded in the URL for shareable investigation links;
- explicit UTC/local timestamp presentation;
- request cancellation when filters change;
- no raw payload column in the table;
- export disabled until authorization, row limit, redaction, and audit rules exist.

### Page: Event detail

Sections:

1. identity: event ID, tenant, type, schema, source;
2. chronology: occurred, accepted/received, materialized, last seen;
3. ordering: key and sequence;
4. processing: current state, duplicate count, identity-conflict warning;
5. payload: formatted, collapsed by default, redacted by backend policy;
6. Kafka metadata when the API exposes it safely: topic, partition, offset;
7. future delivery fan-out and trace links.

Conflicting event identity must be a prominent security/correctness warning, not a small badge.

## 7. Later control-plane pages

### Webhook endpoints

- create with an explicit endpoint-verification flow;
- never display stored signing secrets after creation;
- show secret version/rotation state, not secret value;
- show hostname and safe resolved-network classification;
- show recent normalized outcomes and circuit/rate-limit state;
- disable/delete actions require impact preview and confirmation.

### Subscriptions

- versioned editing with event type, filters, endpoint/channel, priority, expiry, and status;
- validation preview using a non-sensitive sample event;
- optimistic concurrency/version check to prevent overwriting another operator's change;
- activation is separate from saving a draft.

### Templates

- draft, validate, preview, activate, roll back;
- channel and locale variants;
- sample-data preview that cannot execute arbitrary code;
- diff between active and proposed version;
- warning when required variables are missing.

### DLQ and replay

- read-only triage first;
- reason/stage/schema/configuration/attempt summary;
- replay eligibility and why a record is ineligible;
- preview exact scope/count/tenant/channel/expiry/dedupe behavior;
- require typed reason, least privilege, and approval above threshold;
- execute as an asynchronous job with rate, progress, stop, and audit;
- never offer a one-click “replay all” action.

## 8. Component model

Suggested component groups:

```text
ui/src/
  app/               routing, providers, permissions, error boundary
  api/               generated/typesafe client, query keys, error normalization
  components/        buttons, fields, tables, status, dates, code/payload viewer
  features/
    overview/
    events/
    deliveries/
    endpoints/
    subscriptions/
    templates/
    operations/
    audit/
  styles/            tokens, themes, global accessibility rules
  test/              fixtures, mock server, render helpers
```

Rules:

- server state belongs in the query/cache layer, not copied into global UI state;
- form state remains local to the feature;
- permission checks are centralized and backed by server enforcement;
- status labels use one shared mapping from backend machine value to user language;
- destructive/action components always support disabled, submitting, failed, and completed states;
- payload/error viewers receive already-redacted data and still protect against unsafe HTML.

## 9. Backend API contracts needed by the UI

The proposed Phase 1 `GET /api/v1/events/{eventId}` supports an exact detail lookup. UI-1 also needs:

```text
GET /api/v1/events?tenantId=&eventType=&source=&orderingKey=&status=&conflict=&from=&to=&cursor=&limit=
GET /api/v1/overview?tenantId=&window=
```

Later contracts:

```text
GET /api/v1/deliveries
GET /api/v1/deliveries/{deliveryId}
GET/POST/PATCH /api/v1/endpoints
GET/POST/PATCH /api/v1/subscriptions
GET/POST /api/v1/templates and /versions
GET /api/v1/dlq
POST /api/v1/replays/preview
POST /api/v1/replays
GET /api/v1/replays/{jobId}
POST /api/v1/replays/{jobId}/stop
GET /api/v1/audit
```

Every list contract must define cursor semantics, maximum page size, stable sort, tenant scope, redaction, allowed filters, and rate limits. Every command needs idempotency, optimistic concurrency where appropriate, audit reason, and machine-readable error codes.

## 10. UI state requirements

Every page/feature must deliberately design:

- initial loading;
- background refresh without wiping current content;
- empty result versus “not configured yet”;
- validation error;
- unauthenticated and forbidden;
- dependency unavailable;
- stale/partial data;
- rate limited with retry guidance;
- network timeout/unknown outcome;
- success confirmation and correlation/audit ID;
- destructive-command conflict or already-completed result.

## 11. Accessibility and responsive requirements

- Target WCAG 2.2 AA.
- All workflows usable with keyboard only and visible focus.
- Semantic headings, landmarks, tables, buttons, labels, and live regions.
- Status never communicated only through color.
- Color contrast and focus contrast tested automatically and manually.
- Tables support narrow screens through deliberate column priority or detail cards; no essential off-screen-only action.
- Date/time and numbers are readable by assistive technology.
- Payload/code viewers remain navigable and do not trap keyboard focus.
- Motion respects reduced-motion preferences.

## 12. Security and privacy requirements

- Treat all event/template/provider content as untrusted text; never render arbitrary HTML.
- Backend performs redaction and authorization; frontend adds defense in depth.
- Avoid sensitive data in URLs, browser storage, analytics, error trackers, and console logs.
- CSRF/session, token storage, CSP, clickjacking, dependency, and supply-chain controls must match deployment mode.
- Privileged operations show tenant/scope/count and require explicit confirmation.
- UI telemetry must not record raw payloads, addresses, tokens, endpoint secrets, or provider responses containing PII.
- Automatic refresh stops or slows on hidden tabs and handles expired sessions safely.

## 13. UI work packets for AI agents

Budgets are guidance for one focused agent turn, including tests and concise documentation. If a task exceeds its budget, stop at a coherent boundary and report remaining work; do not silently broaden scope.

| ID | Work packet | Depends on | Exclusive paths | Suggested budget | Acceptance |
|---|---|---|---|---:|---|
| UI-001 | Scaffold React/TypeScript app, lint/test/build, app shell, router, error boundary | UI architecture approval | `ui/*` root/config, `ui/src/app/**` | 10k | Container/local build and smoke test pass. |
| UI-002 | Design tokens and accessible primitive components | UI-001, UX-002 | `ui/src/components/**`, `ui/src/styles/**` | 12k | Keyboard, contrast, state stories/tests pass. |
| UI-003 | Typed API client, auth/error normalization, mock server fixtures | UI-001, API contract | `ui/src/api/**`, `ui/src/test/**` | 10k | Contract fixtures cover success/errors/cancellation. |
| UI-004 | Event list with URL filters and cursor pagination | UI-002/003, BE-SEARCH | `ui/src/features/events/list/**` | 14k | Filter, pagination, loading/empty/error, accessibility tests pass. |
| UI-005 | Event detail with chronology/conflict/payload redaction display | UI-002/003 | `ui/src/features/events/detail/**` | 10k | Exact lookup and all UI states tested. |
| UI-006 | Overview summaries and deep links | UI-002/003, BE-OVERVIEW | `ui/src/features/overview/**` | 10k | Time range/staleness/degraded states visible and tested. |
| UI-007 | Endpoint/subscription administration | Phase 2 APIs | `ui/src/features/endpoints/**`, `subscriptions/**` | 18k | Version conflicts, verification, permissions, impact confirmations tested. |
| UI-008 | DLQ/replay preview and job progress | Phase 2 operations APIs, UX-004 | `ui/src/features/operations/**` | 18k | No execution without preview/reason/permission; browser tests pass. |
| UI-009 | Template lifecycle and safe preview | Phase 3 APIs | `ui/src/features/templates/**` | 18k | Draft/diff/validate/activate/rollback and unsafe content tests pass. |

Do not assign two agents the same exclusive path concurrently. Shared-file changes such as navigation, route registration, or generated API types should be owned by an integration agent after feature branches/workspaces are reconciled.

## 14. UI definition of done

- Backend contract exists and is not guessed.
- Permission and tenant scope are test-covered.
- Loading, empty, error, stale, forbidden, and success states exist.
- Keyboard and screen-reader semantics are reviewed.
- No secret/PII leakage in UI, telemetry, URL, or browser storage.
- Component and browser tests pass.
- Build is reproducible and dependency risks are reviewed.
- Operational action is idempotent/audited and communicates unknown outcomes safely.
