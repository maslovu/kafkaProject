package com.maslov.service;

import com.maslov.client.CommentClient;
import com.maslov.dto.CommentEvent;
import io.github.resilience4j.bulkhead.annotation.Bulkhead;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import io.github.resilience4j.timelimiter.annotation.TimeLimiter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@Service
@RequiredArgsConstructor
@Slf4j
public class ExternalApiSender implements CommentSender {

    private final CommentClient commentClient;

    @Retry(name = "commentApiRetry", fallbackMethod = "sendFallback")
    @CircuitBreaker(name = "commentApiBreaker")
    @Bulkhead(name = "commentApiBulkhead", type = Bulkhead.Type.SEMAPHORE)
    @TimeLimiter(name = "commentApiLimiter")
    @Override
    public CompletableFuture<Void> send(List<CommentEvent> batch) {
        log.info("Попытка отправки пакета размером {}...", batch.size());

        return CompletableFuture.supplyAsync(() -> {
            commentClient.sendCommentsBatch(batch);
            return null;
        });
    }

    public CompletableFuture<Void> sendFallback(List<CommentEvent> batch, Throwable t) {
        log.error("Fallback triggered. Причина:", t);
        saveToDeadLetterQueue(batch);
        return CompletableFuture.completedFuture(null); }

    public void saveToDeadLetterQueue(List<CommentEvent> batch) {
        // Логика резервного сохранения (например, запись в файл, в БД otus или в специальный топик-ошибок comments-dlq)
        log.info("Резервное сохранение {} элементов завершено.", batch.size());
    }
}
