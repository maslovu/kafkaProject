package com.maslov.service;

import com.maslov.dto.CommentEvent;
import io.micrometer.core.instrument.Counter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
public class CommentConsumerService {

    private final Counter dlqMessagesCounter;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    private final ExternalApiSender sender;

    public CommentConsumerService(
            ExternalApiSender sender,
            Counter dlqMessagesCounter,
            KafkaTemplate<String, Object> kafkaTemplate
    ) {
        this.sender = sender;
        this.dlqMessagesCounter = dlqMessagesCounter;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(
            topics = "comments-topic",
            groupId = "comment-group-id-v3",
            concurrency = "3",
            containerFactory = "batchFactory"
    )
    public void listenInBatch(List<ConsumerRecord<String, CommentEvent>> batch) {
        log.info("Got batch with size: {}", batch.size());

        List<CommentEvent> events = new ArrayList<>();

        for (int i = 0; i < batch.size(); i++) {
            var event = processRecord(batch.get(i), i);
            if (event != null) {
                events.add(event);
            }
        }

        // Отправляем батч и ЖДЕМ результата
        sendBatch(events, batch.getFirst().topic());
    }

    private CommentEvent processRecord(ConsumerRecord<String, CommentEvent> record, int index) {
        try {
            // Бизнес-логика обработки конкретного сообщения
            return record.value();
        } catch (Exception e) {
            log.error("Error processing message at index {}", index, e);

            // 1. Увеличиваем счетчик ВРУЧНУЮ прямо в момент перехвата
            dlqMessagesCounter.increment();

            sendToDlt(record.topic(), record.key(), null);
        }
        return null;
    }

    private void sendBatch(List<CommentEvent> events, String topic) {
        try {
            // Используем .join() или .get(), чтобы сделать вызов синхронным для потока Kafka
            sender.send(events).join();
            log.info("Package sends successfully. Size: {}", events.size());
        } catch (Exception e) {
            log.error("Global error when sending a batch via the sender. Sending clean batch to DLT.", e);

            dlqMessagesCounter.increment(events.size());

            // отправляем весь этот батч в DLT поштучно, так как sender его не принял
            for (CommentEvent event : events) {
                try {
                    // В качестве ключа можно генерировать UUID или использовать id объекта, если он есть
                    String fallbackKey = event.getBookId() != null ? event.getBookId().toString() : "global-err";

                    sendToDlt(topic, fallbackKey, event);
                } catch (Exception dltEx) {
                    log.error("Критическая ошибка при попытке эвакуации сообщения в DLT: {}", event, dltEx);
                }
            }
            log.warn("Батч размером {} успешно эвакуирован в DLT из-за падения сендера.", events.size());
        }
    }

    private void sendToDlt(String topic, String key, CommentEvent event) {
        String dltTopic = topic + ".DLT";
        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(dltTopic, key, event);
        future.whenComplete((result, ex) -> {
            if (ex != null) {
                // Ошибка
                log.error("Failed to send message with key {} to DLT topic {}: {}",
                        key, dltTopic, ex.getMessage(), ex);
            } else {
                // Успех
                log.info("Message with key {} isolated into DLT topic {} (Partition: {}, Offset: {})",
                        key, dltTopic, result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
            }
        });
    }
}
