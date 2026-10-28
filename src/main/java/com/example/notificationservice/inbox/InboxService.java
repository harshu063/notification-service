package com.example.notificationservice.inbox;

import com.example.notificationservice.events.EventEnvelope;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Clock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class InboxService {
    private final InboxEventRepository repository;
    private final ObjectMapper objectMapper;
    private final Clock clock;

    @Autowired
    public InboxService(InboxEventRepository repository, ObjectMapper objectMapper) {
        this(repository, objectMapper, Clock.systemUTC());
    }

    InboxService(InboxEventRepository repository, ObjectMapper objectMapper, Clock clock) {
        this.repository = repository;
        this.objectMapper = objectMapper;
        this.clock = clock;
    }

    @Transactional
    public boolean accept(EventEnvelope event) {
        event.validate();
        if (repository.existsById(event.eventId())) return false;
        repository.saveAndFlush(new InboxEvent(
                event.eventId(), event.eventType(), event.tenantId(), event.orderingKey(), event.sequence(),
                event.occurredAt(), clock.instant().atOffset(java.time.ZoneOffset.UTC), serialize(event)));
        return true;
    }

    private String serialize(EventEnvelope event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("Event could not be serialized", exception);
        }
    }
}
