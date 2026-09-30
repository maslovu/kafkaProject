package com.maslov.service;

import com.maslov.dto.CommentEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;

@Service
public class KafkaDltConsumerService {

    private static final Logger log = LoggerFactory.getLogger(KafkaDltConsumerService.class);

    @KafkaListener(
            topics = "comments-topic.DLT", // Имя формируется по маске <исходный_топик>.DLT
            groupId = "comment-dlt-group-id",
            containerFactory = "kafkaListenerContainerFactory" // Используем СИНГЛ-фабрику (не батч!)
    )
    public void listenDlt(
            ConsumerRecord<String, CommentEvent> record,
            @Header(name = KafkaHeaders.DLT_ORIGINAL_TOPIC, required = false) byte[] originalTopic,
            @Header(name = KafkaHeaders.DLT_EXCEPTION_MESSAGE, required = false) byte[] exceptionMessage) {

        String topic = originalTopic != null ? new String(originalTopic, StandardCharsets.UTF_8) : "unknown";
        String error = exceptionMessage != null ? new String(exceptionMessage, StandardCharsets.UTF_8) : "unknown";

        log.error("DLT Event: Топик [{}], Ошибка [{}], Ключ [{}], Тело: {}",
                topic, error, record.key(), record.value());

        try {
            // Вызываем логику долгосрочного сохранения (БД, ELK, S3)
            saveToAuditLog(record.value(), error, topic);
        } catch (Exception e) {
            // ВАЖНО: Перехватываем ВСЕ ошибки.
            // Консьюмер DLT ни в коем случае не должен выбрасывать исключение наверх,
            // иначе мы уйдем в бесконечный цикл обработки этой же битой записи.
            log.error("Критическая ошибка при попытке залогировать DLT-сообщение!", e);
        }
    }

    private void saveToAuditLog(CommentEvent event, String error, String originalTopic) {
        // Здесь может быть репозиторий для сохранения в Postgres (например, в таблицу broken_events)
        log.info("Сообщение успешно сохранено в аудит-лог базы данных для ручного разбора.");
    }
}