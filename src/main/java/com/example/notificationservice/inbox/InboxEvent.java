package com.example.notificationservice.inbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Table(name = "notification_inbox")
public class InboxEvent {
    @Id
    private UUID eventId;
    @Column(nullable = false, length = 100)
    private String eventType;
    @Column(nullable = false, length = 100)
    private String tenantId;
    @Column(nullable = false, length = 300)
    private String orderingKey;
    @Column(name = "aggregate_sequence")
    private Long sequence;
    @Column(nullable = false)
    private OffsetDateTime occurredAt;
    @Column(nullable = false)
    private OffsetDateTime receivedAt;
    @Column(nullable = false, columnDefinition = "text")
    private String envelope;

    protected InboxEvent() { }

    InboxEvent(UUID eventId, String eventType, String tenantId, String orderingKey, Long sequence,
               OffsetDateTime occurredAt, OffsetDateTime receivedAt, String envelope) {
        this.eventId = eventId;
        this.eventType = eventType;
        this.tenantId = tenantId;
        this.orderingKey = orderingKey;
        this.sequence = sequence;
        this.occurredAt = occurredAt;
        this.receivedAt = receivedAt;
        this.envelope = envelope;
    }

    public UUID getEventId() { return eventId; }
    public String getOrderingKey() { return orderingKey; }
    public Long getSequence() { return sequence; }
}
