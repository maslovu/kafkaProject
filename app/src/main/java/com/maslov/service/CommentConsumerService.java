package com.maslov.service;

import com.maslov.dto.CommentEvent;
import io.micrometer.core.instrument.Counter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Slf4j
@Service
public class CommentConsumerService {

    private final CommentSender sender;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final Counter dlqMessagesCounter;

    private final BlockingQueue<CommentEvent> buffer = new LinkedBlockingQueue<>();
    private static final int BATCH_SIZE_LIMIT = 50;
    private static final String DLT_TOPIC = "comments-topic.DLT";

    public CommentConsumerService(CommentSender sender,
                                  KafkaTemplate<String, Object> kafkaTemplate,
                                  Counter dlqMessagesCounter) {
        this.sender = sender;
        this.kafkaTemplate = kafkaTemplate;
        this.dlqMessagesCounter = dlqMessagesCounter;
    }

    @KafkaListener(
            topics = "comments-topic",
            groupId = "comment-group-id-v3",
            concurrency = "3",
            containerFactory = "singleFactory"
    )
    public void listen(ConsumerRecord<String, CommentEvent> record) throws InterruptedException {
        log.debug("Message received from Kafka. Key: {}", record.key());

        CommentEvent event = record.value();

        if (event != null) {
            buffer.put(event);

            if (buffer.size() >= BATCH_SIZE_LIMIT) {
                flushBuffer();
            }
        }
    }

    @Scheduled(fixedRate = 5000)
    public void flushBufferScheduled() {
        if (!buffer.isEmpty()) {
            flushBuffer();
        }
    }

    private synchronized void flushBuffer() {
        if (buffer.isEmpty()) {
            return;
        }

        List<CommentEvent> batchToSend = new ArrayList<>();
        buffer.drainTo(batchToSend);

        log.info("Sending the accumulated batch. Size: {}", batchToSend.size());

        try {
            sender.send(batchToSend).join();
            log.info("Batch successfully sent");
        } catch (Exception e) {
            log.error("Error while sending a batch. Evacuation to DLT.", e);

            dlqMessagesCounter.increment(batchToSend.size());

            for (CommentEvent event : batchToSend) {
                String fallbackKey = event.getBookId() != null ? event.getBookId().toString() : "bulk-err";
                kafkaTemplate.send(DLT_TOPIC, fallbackKey, event);
            }
        }
    }
}
