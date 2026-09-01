package com.maslov.service;

import com.maslov.CommentClient;
import com.maslov.dto.CommentEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Slf4j
@Service
@RequiredArgsConstructor
public class CommentConsumer {

    private final CommentClient commentClient;

    private final BlockingQueue<CommentEvent> buffer = new LinkedBlockingQueue<>();

    private static final int BATCH_SIZE_THRESHOLD = 50;

    @KafkaListener(
            topics = "comments-topic",
            groupId = "comment-group-id-v3",
            concurrency = "3",
            containerFactory = "batchFactory"
    )
    public void listenInBatch(ConsumerRecords<String, CommentEvent> records) {
        log.info("Got batch with size: " + records.count());

        for (ConsumerRecord<String, CommentEvent> record : records) {
            buffer.add(record.value());
        }

        if (buffer.size() >= BATCH_SIZE_THRESHOLD) {
            flushBuffer();
        }
    }

    private synchronized void flushBuffer() {
        // Проверяем размер еще раз внутри синхронизированного блока
        if (buffer.size() < BATCH_SIZE_THRESHOLD) {
            return;
        }

        // Извлекаем элементы для отправки в локальный список
        List<CommentEvent> batchToSend = new ArrayList<>();
        buffer.drainTo(batchToSend, BATCH_SIZE_THRESHOLD);

        try {
            log.info("Отправка пакета из {} комментариев через Feign...", batchToSend.size());
            commentClient.sendCommentsBatch(batchToSend);
            log.info("Пакет успешно доставлен.");
        } catch (Exception e) {
            log.error("Ошибка отправки пакета! Возвращаем элементы обратно в буфер для повторной попытки", e);
            throw e; // Пробрасываем ошибку, чтобы Kafka не сдвигала Offset (Ack)
        }
    }
}