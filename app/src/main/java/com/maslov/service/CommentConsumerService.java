package com.maslov.service;

import com.maslov.dto.CommentEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
public class CommentConsumerService {

    private final ExternalApiSender sender;

    public CommentConsumerService(ExternalApiSender sender) {
        this.sender = sender;
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
            try {
                // Бизнес-логика обработки конкретного сообщения
                events.add(record.value());
            } catch (Exception e ) {
                log.error("Ошибка при обработке сообщения на индексе {}", i, e);

                // ВАЖНО: Выбрасываем BatchListenerFailedException, передавая исключение и индекс элемента!
                // Spring зафиксирует это, отправит в DLT только ЭТОТ элемент, а остальные пропустит
                throw new BatchListenerFailedException("Error processing record in batch", e, i);
            }
        }

        // Отправляем батч и ЖДЕМ результата
        if (!events.isEmpty()) {
            try {
                // Используем .join() или .get(), чтобы сделать вызов синхронным для потока Kafka
                sender.send(events).join();

                log.info("Package sends successfully");
            } catch (Exception e) {
                log.error("Global error when sending a batch via the sender", e);

                // Пробрасываем ошибку дальше.
                // Если в sender.send() НЕ отработал локальный fallback,
                // то DefaultErrorHandler перехватит эту ошибку и отправит ВЕСЬ этот батч в Kafka DLT
                throw e;
            }
        }
    }
}
