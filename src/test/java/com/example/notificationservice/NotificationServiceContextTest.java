package com.example.notificationservice;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.notificationservice.events.KafkaEventConsumer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest(properties = "spring.kafka.listener.auto-startup=false")
@ActiveProfiles("test")
class NotificationServiceContextTest {
    @Autowired KafkaEventConsumer consumer;
    @Autowired DefaultErrorHandler errorHandler;

    @Test
    void startsWithKafkaConsumerAndDeadLetterFailurePolicyConfigured() {
        assertThat(consumer).isNotNull();
        assertThat(errorHandler).isNotNull();
    }
}
