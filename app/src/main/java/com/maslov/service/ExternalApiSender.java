package com.maslov.service;

import com.maslov.client.CommentClient;
import com.maslov.dto.CommentEvent;
import io.github.resilience4j.bulkhead.annotation.Bulkhead;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import io.github.resilience4j.timelimiter.annotation.TimeLimiter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

@Service
@RequiredArgsConstructor
@Slf4j
public class ExternalApiSender implements CommentSender {

    private final CommentClient commentClient;
    private final Executor httpExecutor;

    @CircuitBreaker(name = "commentApiBreaker", fallbackMethod = "sendFallback")
    @Retry(name = "commentApiRetry")
    @Bulkhead(name = "commentApiBulkhead", type = Bulkhead.Type.SEMAPHORE)
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
        // Логика резервного сохранения (например, запись в файл, в БД otus или в специальный топик-ошибок comments-topic.DLT)
        log.info("Backup of {} elements completed", batch.size());
    }
}
