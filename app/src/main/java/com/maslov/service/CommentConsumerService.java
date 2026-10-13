package com.maslov.service;

import com.maslov.dto.CommentEvent;
import io.micrometer.core.instrument.Counter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

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
            ConsumerRecord<String, CommentEvent> record = batch.get(i);
            CommentEvent event = null;
            try {
                // Бизнес-логика обработки конкретного сообщения
                event = record.value();
                events.add(event);
            } catch (Exception e ) {
                log.error("Error processing message at index {}", i, e);

                // 1. Увеличиваем счетчик ВРУЧНУЮ прямо в момент перехвата
                dlqMessagesCounter.increment();

                String dltTopic = record.topic() + ".DLT"; // Получится comments-topic.DLT
                kafkaTemplate.send(dltTopic, record.key(), event);
                log.info("The message with key {} has been successfully isolated into DLT topic {}", record.key(), dltTopic);
            }
        }

        // Отправляем батч и ЖДЕМ результата
        if (!events.isEmpty()) {
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
                        // Используем имя топика (можно захардкодить или вытащить из первого record)
                        String dltTopic = "comments-topic.DLT";

                        // В качестве ключа можно генерировать UUID или использовать id объекта, если он есть
                        String fallbackKey = event.getBookId() != null ? event.getBookId().toString() : "global-err";

                        kafkaTemplate.send(dltTopic, fallbackKey, event);
                    } catch (Exception dltEx) {
                        log.error("Критическая ошибка при попытке эвакуации сообщения в DLT: {}", event, dltEx);
                    }
                }
                log.warn("Батч размером {} успешно эвакуирован в DLT из-за падения сендера.", events.size());
            }
        }
    }
}
