CREATE TABLE notification_inbox (
    event_id UUID PRIMARY KEY,
    event_type VARCHAR(100) NOT NULL,
    tenant_id VARCHAR(100) NOT NULL,
    ordering_key VARCHAR(300) NOT NULL,
    aggregate_sequence BIGINT,
    occurred_at TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL,
    envelope TEXT NOT NULL,
    CONSTRAINT ck_notification_inbox_sequence CHECK (aggregate_sequence IS NULL OR aggregate_sequence >= 0)
);

CREATE INDEX ix_notification_inbox_ordering
    ON notification_inbox (tenant_id, ordering_key, aggregate_sequence);
CREATE UNIQUE INDEX ux_notification_inbox_ordering_sequence
    ON notification_inbox (tenant_id, ordering_key, aggregate_sequence)
    WHERE aggregate_sequence IS NOT NULL;
CREATE INDEX ix_notification_inbox_received
    ON notification_inbox (received_at);
