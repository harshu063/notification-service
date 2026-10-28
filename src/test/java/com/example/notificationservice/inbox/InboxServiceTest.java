package com.example.notificationservice.inbox;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.notificationservice.events.EventEnvelope;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.Import;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

@DataJpaTest(properties = "spring.profiles.active=test")
@Import({InboxService.class, InboxServiceTest.JsonConfiguration.class})
class InboxServiceTest {
    @Autowired InboxEventRepository repository;
    @Autowired InboxService inboxService;

    @TestConfiguration
    static class JsonConfiguration {
        @Bean
        com.fasterxml.jackson.databind.ObjectMapper objectMapper() {
            return new com.fasterxml.jackson.databind.ObjectMapper().findAndRegisterModules();
        }
    }

    @Test
    void persistsFirstDeliveryAndIgnoresRedeliveryByEventId() {
        UUID id = UUID.randomUUID();
        EventEnvelope event = new EventEnvelope(id, "order.created.v1", 1, "tenant-a", "order-service",
                new EventEnvelope.Subject("order", "order-42"), "tenant-a:order-42", 1L,
                OffsetDateTime.parse("2026-10-01T12:00:00Z"), "correlation-1", null, null, null,
                JsonNodeFactory.instance.objectNode());

        assertThat(inboxService.accept(event)).isTrue();
        assertThat(inboxService.accept(event)).isFalse();
        assertThat(repository.count()).isEqualTo(1);
        assertThat(repository.findById(id)).get().extracting(InboxEvent::getOrderingKey)
                .isEqualTo("tenant-a:order-42");
    }
}
