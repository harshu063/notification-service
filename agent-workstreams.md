# AI Agent Workstreams and Token-Budget Guide

**Purpose:** enable multiple AI agents to work concurrently without duplicating work, changing contracts independently, or corrupting shared files.

This file is a coordination contract. It does not grant an agent permission to deploy, contact external users/providers, alter production, or perform destructive operations.

## 1. Operating model

Use one coordinator/integration agent and several bounded implementation agents.

```text
Coordinator
  |-- Architecture/contract agent
  |-- Backend feature agent
  |-- UI feature agent
  |-- UX/research-design agent
  |-- QA/automation agent
  +-- Security/performance specialist agents when their phase begins
```

The coordinator owns sequencing, shared contracts, cross-workstream decisions, integration, and final verification. Agents should not all edit `README.md`, `phases.md`, navigation, root build files, API schemas, or shared migrations concurrently.

## 2. Token budget bands

Token budgets are planning caps, not quality substitutes.

| Band | Suggested budget | Appropriate work |
|---|---:|---|
| XS | 2k–4k | One review, tiny documentation update, one narrow test/fix. |
| S | 4k–8k | One bounded component, ADR, API contract review, research plan. |
| M | 8k–14k | Feature slice with unit tests, one UI page, focused integration setup. |
| L | 14k–22k | Multi-file vertical slice, Testcontainers suite, complex safe workflow. |
| XL | 22k–35k | Load harness, provider adapter with resilience, broad migration; split when possible. |

If an agent receives less than the suggested budget, reduce scope explicitly. Never compensate by omitting tests, security boundaries, or reporting.

## 3. Required task packet

Every assignment should include:

```text
Task ID and objective:
Why it matters / requirement IDs:
Inputs to read:
Exclusive files/directories allowed to edit:
Files owned by others (read-only):
Contract/API/schema it must follow:
Acceptance criteria:
Tests/checks to execute:
Suggested token budget:
Out of scope:
Required handoff format:
```

Agents must read the applicable specification completely before acting.

## 4. Recommended workstream ownership

| Workstream | Primary paths | Shared-contract restriction |
|---|---|---|
| Architecture | `architecture.md`, `docs/adr/**`, schemas proposed for review | Does not implement feature code unless assigned separately. |
| Backend | `src/main/java/**`, backend tests assigned to it | Cannot change public API/topic schema without contract approval. |
| Database | `src/main/resources/db/migration/**`, repository integration tests | One migration owner at a time; never edit an applied migration. |
| UI | `ui/src/features/<assigned-feature>/**` | App shell/routes/shared API client have designated owners. |
| UX | `docs/ux/**`, prototypes/assets assigned | Must label assumptions; cannot claim research not performed. |
| QA | assigned `src/test`, `ui/e2e`, or `qa/<area>` path | Should not rewrite production code except a separately accepted fix. |
| Security | `qa/security/**`, threat-model/ADR inputs | Reports findings; fixes assigned explicitly by owner. |
| Performance | `qa/load/**`, reports | Cannot redefine workload/SLO to make a test pass. |
| Integration | shared root config, navigation, contract generation, final merges | Only coordinator/integration agent owns during integration window. |

## 5. Dependency sequence

```text
Requirement decision
      |
      v
API/event/state contract + ADR
      |--------------------------|
      v                          v
Backend implementation       UX flow/states
      |                          |
      +------------+-------------+
                   v
              UI feature
                   |
                   v
       Contract/integration/E2E QA
                   |
                   v
        Performance/security/ops gates
```

Work may overlap only when the input contract is stable or explicitly mocked/versioned.

## 6. Initial agent queue

### Ready now

| Order | Task | Budget | Owner type | Notes |
|---:|---|---:|---|---|
| 1 | BE-000: scaffold the Phase 1 Spring Boot/Kafka/PostgreSQL service | 18k | Backend agent | Follow `architecture.md`; documentation approval comes first. |
| 2 | UX-001: operator discovery plan | 6k | UX agent | Can run independently; findings require real sessions. |
| 3 | UX-002: information architecture validation | 8k | UX agent | Can prototype from approved terminology/routes. |
| 4 | API-001: ingestion/status/search contract draft | 10k | Contract agent | Must be approved before backend/UI implementation relies on it. |
| 5 | UI-001: frontend scaffold/app shell | 10k | UI agent | May use contract fixtures; no invented backend fields. |
| 6 | QA-PLAN-001: Phase 1 traceability and executable test design | 8k | QA agent | Produces test plan/fixtures without claiming execution. |

### Ready after dependencies

| Task | Depends on | Budget |
|---|---|---:|
| QA-001 Phase 1 unit/API tests | BE-000 + API-001 | 10k |
| QA-002 real Kafka/PostgreSQL integration suite | BE-000 | 16k |
| BE-001 event search/cursor implementation | API-001 + BE-000 | 14k |
| BE-002 overview aggregation implementation | API-001 + BE-000 | 10k |
| UI-002 accessible primitives | UI-001 + UX-002 | 12k |
| UI-003 API client/mock fixtures | approved API contract | 10k |
| UI-004 event list | BE-001 + UI-002/003 | 14k |
| UI-005 event detail | UI-002/003 | 10k |
| UI-006 overview | BE-002 + UI-002/003 | 10k |
| QA-005 UI accessibility harness | UI-001/002 | 10k |
| QA-006 event browser journey | UI-004/005 + test backend | 10k–16k |

Phase 2 channel/replay tasks remain blocked until endpoint, subscription, delivery-state, retry, and security contracts are approved.

## 7. Handoff format

Every agent returns:

1. outcome in one paragraph;
2. files changed;
3. contract/schema/behavior decisions made;
4. tests/checks actually run with results;
5. assumptions and unverified claims;
6. remaining risks/TODOs;
7. recommended next task and dependency.

“Implemented” means code exists and relevant checks ran. If execution was impossible, say “authored but not executed.”

## 8. Conflict-avoidance rules

- Assign exclusive paths before spawning work.
- One owner edits a database migration or API schema at a time.
- Do not have feature agents modify root configuration simultaneously.
- Prefer additive files and small patches.
- Agents inspect current files before editing because another agent may have completed related work.
- Never discard unknown changes; escalate overlap to the coordinator.
- Integrate in dependency order, then run the combined build and contract tests.
- A failing integration is not solved by weakening an acceptance criterion without stakeholder approval.

## 9. Review gates for AI-generated work

- Architecture/contract review before implementation that affects public APIs, topics, identity, ordering, retries, security, or data ownership.
- Human/domain review for user-facing language and provider/legal semantics.
- Security review for authentication, webhooks, secrets, callbacks, replay, exports, and PII.
- Executed tests for code claims.
- Load evidence before scale claims.
- Operator rehearsal before enabling high-impact controls.

## 10. Coordinator completion checklist

- All work packets stayed within assigned paths/scope.
- Contracts and generated clients are consistent.
- Migrations are ordered, immutable, and validated.
- Unit, integration, UI, security, and relevant load checks ran.
- Documentation reflects implemented—not aspirational—behavior.
- No secrets, PII, temporary files, or agent artifacts were committed.
- A clear next queue exists with dependencies and budgets.
