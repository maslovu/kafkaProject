package com.maslov.service;

import com.maslov.dto.CommentEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Slf4j
@Service
public class CommentConsumerService {

    private final CommentSender sender;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    // Храним и само событие, и его Acknowledgment, чтобы закоммитить оффсет только после отправки
    private record QueuedItem(ConsumerRecord<String, CommentEvent> record, Acknowledgment ack) {}

    private final BlockingQueue<QueuedItem> buffer = new LinkedBlockingQueue<>();
    private static final int BATCH_SIZE_LIMIT = 50;
    private static final String DLT_TOPIC = "comments-topic.DLT";

    public CommentConsumerService(CommentSender sender,
                                  KafkaTemplate<String, Object> kafkaTemplate) {
        this.sender = sender;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(
            topics = "comments-topic",
            groupId = "comment-group-id",
            concurrency = "3",
            containerFactory = "singleFactory"
    )
    public void listen(ConsumerRecord<String, CommentEvent> record, Acknowledgment ack) throws InterruptedException {
        log.debug("Message received from Kafka. Key: {}", record.key());

        if (record.value() == null) {
            ack.acknowledge(); // Битое сообщение (после ErrorHandlingDeserializer) сразу подтверждаем
            return;
        }

        buffer.put(new QueuedItem(record, ack));

        if (buffer.size() >= BATCH_SIZE_LIMIT) {
            flushBuffer();
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

        List<QueuedItem> items = new ArrayList<>();
        buffer.drainTo(items, BATCH_SIZE_LIMIT);

        List<CommentEvent> batchToSend = items.stream()
                .map(item -> item.record().value())
                .toList();

        log.info("Sending the accumulated batch. Size: {}", batchToSend.size());

        try {
            sender.send(batchToSend).join();
            log.info("Batch successfully sent");
        } catch (Exception e) {
            log.error("Error while sending a batch. Evacuation to DLT.", e);

            for (QueuedItem item : items) {
                CommentEvent event = item.record().value();
                String key = item.record().key() != null
                        ? item.record().key()
                        : (event.getBookId() != null ? event.getBookId().toString() : "bulk-err");
                //гарантирует, что сообщение реально ушло в DLT до того, как мы закоммитим оффсет
                ProducerRecord<String, Object> dltRecord = new ProducerRecord<>(DLT_TOPIC, key, event);
                dltRecord.headers().add(KafkaHeaders.DLT_ORIGINAL_TOPIC, "comments-topic".getBytes(StandardCharsets.UTF_8));
                dltRecord.headers().add(KafkaHeaders.DLT_EXCEPTION_MESSAGE,
                        (e.getMessage() != null ? e.getMessage() : e.getClass().getName()).getBytes(StandardCharsets.UTF_8));
                kafkaTemplate.send(dltRecord).whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to send to DLT: {}", ex.getMessage(), ex);
                    }
                });
            }
        }

        // Коммитим оффсеты только после успешной отправки в API или гарантированной записи в DLT
        items.forEach(item -> item.ack().acknowledge());
    }
}
