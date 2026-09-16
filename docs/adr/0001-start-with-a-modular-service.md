# ADR 0001: Start with a modular Spring Boot service

- **Status:** Accepted
- **Date:** 2026-08-27

## Context

The target notification platform has logical ingestion, routing, template, preference, webhook, email, push, status, and retry components. Deploying all of them independently before their contracts and load are proven would create distributed operational complexity without yet creating business value.

## Decision

Implement the initial vertical slices in one Spring Boot repository/runtime with explicit package and Kafka contract boundaries. Use Kafka as the durable asynchronous backbone and PostgreSQL as the queryable state/deduplication store. Extract channel workers and routing into separate deployments when scaling, isolation, security, or team ownership requires it.

## Consequences

Positive:

- faster learning and integration testing;
- local transactions remain available for state changes;
- fewer deployments and network failure modes initially;
- future extraction seams remain explicit through topics and module boundaries.

Negative:

- one runtime initially shares failure and deployment blast radius;
- module discipline must be enforced through tests/reviews;
- scale tests may later force independent ingestion/router/channel processes;
- an extraction will require schema/API ownership work.

## Extraction triggers

- one channel's latency or quota behavior threatens another;
- routing fan-out scales differently from ingestion;
- security boundary requires controlled webhook egress;
- independent teams need release ownership;
- profiling shows resource contention that cannot be isolated within one runtime.

