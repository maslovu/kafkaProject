package com.maslov.service;

import com.maslov.dto.CommentEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
public class CommentConsumer {

    private final ExternalApiSender sender;

    public CommentConsumer(ExternalApiSender sender) {
        this.sender = sender;
    }

    @KafkaListener(
            topics = "comments-topic",
            groupId = "comment-group-id-v3",
            concurrency = "3",
            containerFactory = "batchFactory"
    )
    public void listenInBatch(ConsumerRecords<String, CommentEvent> records) {
        if (records.isEmpty() || records.count() == 0) {
            return;
        }

        log.info("Got batch with size: {}", records.count());


        List<CommentEvent> events = new ArrayList<>();

        for (ConsumerRecord<String, CommentEvent> record : records) {
            if (record.value() != null) {
                events.add(record.value());
                log.info("Filtered non-null messages count: {}", events.size());
            }
        }

        if (!events.isEmpty()) {
            sender.send(events);
            log.info("Package sends successfully");
        } else {
            log.error("Received empty or null-only batch from Kafka.");
        }
    }
}
