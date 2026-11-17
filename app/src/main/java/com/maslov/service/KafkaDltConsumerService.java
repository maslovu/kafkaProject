package com.maslov.service;

import com.maslov.dto.CommentEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.SerializationUtils;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;

@Service
@Slf4j
public class KafkaDltConsumerService {

    @KafkaListener(
            topics = "comments-topic.DLT",
            groupId = "comment-dlt-group-id",
            containerFactory = "dltFactory"
    )
    public void listenDlt(
            ConsumerRecord<String, CommentEvent> record,
            @Header(name = KafkaHeaders.DLT_ORIGINAL_TOPIC, required = false) byte[] originalTopic,
            @Header(name = KafkaHeaders.DLT_EXCEPTION_MESSAGE, required = false) byte[] exceptionMessage,
            @Header(name = SerializationUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER, required = false) byte[] deserializerEx) {

        String topic = originalTopic != null ? new String(originalTopic, StandardCharsets.UTF_8) : "unknown";
        String error = exceptionMessage != null ? new String(exceptionMessage, StandardCharsets.UTF_8) : "unknown";

        log.error("DLT Event: Topic [{}], Error [{}], Key [{}], Body: {}",
                topic, error, record.key(), record.value());

        CommentEvent event = record.value();
        if (event != null) {
            // Пришел валидный CommentEvent (ошибка была при отправке по HTTP)
            log.error("DLT Event (Valid Object): Topic [{}], Error [{}], Key [{}], Event: {}",
                    topic, error, record.key(), event);
        } else {
            // Пришел битый JSON (ошибка была при десериализации)
            log.error("DLT Event (Corrupted JSON): Topic [{}], Error [{}], Key [{}]",
                    topic, error, record.key());
        }

        try {
            saveToAuditLog(event, error, topic);
        } catch (Exception e) {
            log.error("Critical error while attempting to log a DLT message", e);
        }
    }

    private void saveToAuditLog(CommentEvent event, String error, String originalTopic) {
        // Здесь может быть репозиторий для сохранения в Postgres (например, в таблицу broken_events)
        log.info("The message has been successfully saved to the database audit log for manual analysis");
    }
}
