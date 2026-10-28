# Platform contracts v1

These contracts are a review baseline. `x-implementation-status` distinguishes verified endpoints from proposed ones; a contract does not mean the service exists.

## Files

- `openapi/platform-api-v1.yaml` — browser/gateway HTTP contract.
- `events/event-envelope-v1.schema.json` — common Kafka event envelope.
- `events/order-created-v1.schema.json` — Order-created payload/envelope.
- `events/payment-succeeded-v1.schema.json` — Payment-success payload/envelope.
- `asyncapi/platform-events-v1.yaml` — initial Kafka channels and operations.

## Governance

- API base path is `/api/v1`; breaking HTTP changes require `/api/v2` or a migration-compatible rollout.
- Event names and payload schema versions are explicit; consumers must tolerate additive optional fields.
- Every command carries `Idempotency-Key`; every response/problem carries `X-Correlation-Id`.
- Kafka records use the canonical envelope and stable event ID; order/payment keys are `tenantId:orderId`.
- OpenAPI is the frontend/client source. Persistence entities are not contracts.
- Contract CI must lint OpenAPI/AsyncAPI, validate JSON Schema examples and run compatibility checks before generation.

## Validation

Run from this directory:

```powershell
uv run --script validate_contracts.py
```

The pinned, ephemeral validator checks the complete OpenAPI 3.1 document, resolves every AsyncAPI reference and enforces the platform's event/topic versioning rules, compiles all Draft 2020-12 event schemas, and validates the committed examples. This is the local/CI baseline; add backward-compatibility comparison against the last released contracts when a release registry exists.
