package com.example.notificationservice.events;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JsonConfiguration {
    @Bean
    ObjectMapper notificationObjectMapper() {
        return new ObjectMapper().findAndRegisterModules();
    }
}
