package com.maslov.service;

import com.maslov.client.CommentClient;
import com.maslov.dto.CommentEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class CommentConsumer {

    private final List<CommentEvent> buffer = new ArrayList<>();

    CommentClient commentClient;

    @KafkaListener(
            topics = "comments-topic",
            groupId = "comment-group-id",
            concurrency = "3",
            containerFactory = "batchFactory"
    )
    public void listenInBatch(ConsumerRecords<String, CommentEvent> records) {
        System.out.println("Got batch with size: " + records.count());

        for (ConsumerRecord<String, CommentEvent> record : records) {
            buffer.add(record.value());
        }

        if (buffer.size() >= 50) {
            flushBuffer();
        }
    }

    private synchronized void flushBuffer() {
        try {
            commentClient.sendCommentsBatch(buffer);
        } finally {
            buffer.clear(); // Очищаем буфер после попытки отправки } }
        }
    }
}