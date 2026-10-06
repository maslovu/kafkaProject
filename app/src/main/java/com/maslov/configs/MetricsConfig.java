package com.maslov.configs;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MetricsConfig {

    @Bean
    public Counter dlqMessagesCounter(MeterRegistry meterRegistry) {
        return Counter.builder("kafka.dlq.messages.count")
                .description("Number of messages sent to the Kafka DLT")
                .tags("topic", "comments-topic.DLT")
                .register(meterRegistry);
    }
}