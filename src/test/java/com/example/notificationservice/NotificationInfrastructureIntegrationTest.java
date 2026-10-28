package com.example.notificationservice;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.example.notificationservice.events.EventEnvelope;
import com.example.notificationservice.inbox.InboxEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaAdmin.NewTopics;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(properties = {
        "notification.kafka.topic=notification.integration.events",
        "notification.kafka.group-id=notification-integration-test",
        "notification.kafka.concurrency=1",
        "spring.kafka.consumer.auto-offset-reset=earliest"
})
@Import(NotificationInfrastructureIntegrationTest.TopicConfiguration.class)
class NotificationInfrastructureIntegrationTest {
    private static final String EVENTS_TOPIC = "notification.integration.events";
    private static final String DLT_TOPIC = EVENTS_TOPIC + ".DLT";

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:17.2")
            .withDatabaseName("notification_service")
            .withUsername("notification_user")
            .withPassword("notification_test_password");

    @Container
    static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.0"));

    @DynamicPropertySource
    static void infrastructureProperties(DynamicPropertyRegistry properties) {
        properties.add("spring.datasource.url", postgres::getJdbcUrl);
        properties.add("spring.datasource.username", postgres::getUsername);
        properties.add("spring.datasource.password", postgres::getPassword);
        properties.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @Autowired KafkaTemplate<String, String> kafkaTemplate;
    @Autowired InboxEventRepository inboxRepository;
    @Autowired ObjectMapper objectMapper;

    @Test
    void migratesPostgresAndPersistsKafkaEventOnlyOnce() throws Exception {
        UUID eventId = UUID.randomUUID();
        EventEnvelope event = validEvent(eventId, "tenant-a:order-42");
        String serializedEvent = objectMapper.writeValueAsString(event);

        kafkaTemplate.send(EVENTS_TOPIC, event.orderingKey(), serializedEvent).get();
        kafkaTemplate.send(EVENTS_TOPIC, event.orderingKey(), serializedEvent).get();

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(inboxRepository.findById(eventId)).isPresent();
            assertThat(inboxRepository.count()).isEqualTo(1);
        });
    }

    @Test
    void routesInvalidOrderingKeyRecordToDeadLetterTopicAfterRetries() throws Exception {
        String orderingKey = "tenant-a:order-99";
        String invalidEnvelope = objectMapper.writeValueAsString(validEvent(UUID.randomUUID(), "wrong-key"));
        try (Consumer<String, String> dltConsumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "notification-dlt-test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class))) {
            dltConsumer.subscribe(List.of(DLT_TOPIC));
            kafkaTemplate.send(EVENTS_TOPIC, orderingKey, invalidEnvelope).get();

            ConsumerRecords<String, String> deadLetterRecords = await()
                    .atMost(Duration.ofSeconds(15))
                    .until(() -> dltConsumer.poll(Duration.ofMillis(250)), records -> !records.isEmpty());

            assertThat(deadLetterRecords.iterator().next().value()).isEqualTo(invalidEnvelope);
        }
    }

    private EventEnvelope validEvent(UUID eventId, String orderingKey) {
        return new EventEnvelope(eventId, "order.created.v1", 1, "tenant-a", "order-service",
                new EventEnvelope.Subject("order", "order-42"), orderingKey, 1L,
                OffsetDateTime.parse("2026-10-01T12:00:00Z"), "correlation-test", null, null, null,
                objectMapper.createObjectNode().put("orderId", "order-42"));
    }

    @TestConfiguration
    static class TopicConfiguration {
        @Bean
        NewTopics integrationTopics() {
            return new NewTopics(
                    TopicBuilder.name(EVENTS_TOPIC).partitions(1).replicas(1).build(),
                    TopicBuilder.name(DLT_TOPIC).partitions(1).replicas(1).build());
        }
    }
}
