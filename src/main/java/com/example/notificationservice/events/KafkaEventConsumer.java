package com.example.notificationservice.events;

import com.example.notificationservice.inbox.InboxService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.apache.kafka.clients.consumer.ConsumerRecord;

@Component
public class KafkaEventConsumer {
    private final ObjectMapper objectMapper;
    private final InboxService inboxService;

    public KafkaEventConsumer(ObjectMapper objectMapper, InboxService inboxService) {
        this.objectMapper = objectMapper;
        this.inboxService = inboxService;
    }

    @KafkaListener(topics = "${notification.kafka.topic:platform.events.v1}",
            groupId = "${notification.kafka.group-id:notification-service-v1}",
            concurrency = "${notification.kafka.concurrency:3}")
    public void consume(ConsumerRecord<String, String> record) throws Exception {
        EventEnvelope event = objectMapper.readerFor(EventEnvelope.class)
                .with(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .readValue(record.value());
        event.validate();
        if (!event.orderingKey().equals(record.key())) {
            throw new IllegalArgumentException("Kafka record key must match the event orderingKey");
        }
        inboxService.accept(event);
    }
}
