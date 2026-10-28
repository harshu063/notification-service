package com.example.notificationservice.events;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.OffsetDateTime;
import java.util.UUID;

public record EventEnvelope(
        UUID eventId,
        String eventType,
        int schemaVersion,
        String tenantId,
        String source,
        Subject subject,
        String orderingKey,
        Long sequence,
        OffsetDateTime occurredAt,
        String correlationId,
        String causationId,
        String traceparent,
        OffsetDateTime expiresAt,
        JsonNode payload) {

    public void validate() {
        if (eventId == null || eventType == null || eventType.length() > 100
                || !eventType.matches("^[a-z][a-z0-9.-]+\\.v[1-9][0-9]*$")) {
            throw new IllegalArgumentException("eventId and versioned eventType are required");
        }
        if (schemaVersion != 1 || !hasLength(tenantId, 100) || !hasLength(source, 100)
                || subject == null || !hasLength(orderingKey, 300) || occurredAt == null
                || !hasLength(correlationId, 128) || !optionalLength(causationId, 128)
                || !optionalLength(traceparent, 256) || payload == null || !payload.isObject()) {
            throw new IllegalArgumentException("Event does not match the v1 envelope contract");
        }
        if (!hasLength(subject.type(), 100) || !hasLength(subject.id(), 200)) {
            throw new IllegalArgumentException("Event subject type and id are required");
        }
        if (sequence != null && sequence < 0) {
            throw new IllegalArgumentException("Event sequence must be non-negative");
        }
    }

    private boolean hasLength(String value, int max) {
        return value != null && !value.isBlank() && value.length() <= max;
    }

    private boolean optionalLength(String value, int max) {
        return value == null || value.length() <= max;
    }

    public record Subject(String type, String id) { }
}
