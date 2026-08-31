package com.maslov.service;

import com.maslov.client.CommentClient;
import com.maslov.dto.CommentEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class CommentConsumer {

    CommentClient commentClient;

    @KafkaListener(
            topics = "comment-topic",
            groupId = "comment-group-id",
            concurrency = "3"
    )
    public void consume(CommentEvent event) {
        System.out.println("Get message: " + event);

        processComment(event);
    }

    private void processComment(CommentEvent event) {
        commentClient.createComment(event.getBookId(), event.getComment());
    }
}
