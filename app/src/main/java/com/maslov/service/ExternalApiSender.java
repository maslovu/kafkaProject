package com.maslov.service;

import com.maslov.client.CommentClient;
import com.maslov.dto.CommentEvent;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import io.github.resilience4j.timelimiter.annotation.TimeLimiter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

@Service
@Slf4j
public class ExternalApiSender implements CommentSender {

    private final CommentClient commentClient;
    private final Executor httpExecutor;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    private static final String DLT_TOPIC = "comments-topic.DLT";

    public ExternalApiSender(CommentClient commentClient,
                             @Qualifier("httpExecutor") Executor httpExecutor,
                             KafkaTemplate<String, Object> kafkaTemplate) {
        this.commentClient = commentClient;
        this.httpExecutor = httpExecutor;
        this.kafkaTemplate = kafkaTemplate;
    }

    @CircuitBreaker(name = "commentApiBreaker", fallbackMethod = "sendFallback")
    @Retry(name = "commentApiRetry")
    @TimeLimiter(name = "commentApiLimiter")
    @Override
    public CompletableFuture<Void> send(List<CommentEvent> batch) {
        log.info("Attempting to send a packet of size {}...", batch.size());

        return CompletableFuture.supplyAsync(() -> {
            ResponseEntity<Void> response = commentClient.sendCommentsBatch(batch);
            log.debug("Batch send response status: {}", response.getStatusCode());
            return null;
        }, httpExecutor);
    }

    public CompletableFuture<Void> sendFallback(List<CommentEvent> batch, Throwable t) {
        log.error("Fallback triggered. Cause:", t);
        saveToDeadLetterQueue(batch);
        return CompletableFuture.completedFuture(null);
    }

    public void saveToDeadLetterQueue(List<CommentEvent> batch) {
        for (CommentEvent event : batch) {
            String key = event.getBookId() != null ? event.getBookId().toString() : "fallback-err";
            try {
                kafkaTemplate.send(DLT_TOPIC, key, event);
                log.info("Sent {} elements to DLT", batch.size());
            } catch (Exception e) {
                log.error("Failed to send to DLT: {}", e.getMessage());
            }
        }
    }
}
