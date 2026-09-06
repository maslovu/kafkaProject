package com.maslov.service;

import com.maslov.client.CommentClient;
import com.maslov.dto.CommentEvent;
import io.github.resilience4j.bulkhead.annotation.Bulkhead;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator;
import io.github.resilience4j.timelimiter.annotation.TimeLimiter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Service
@RequiredArgsConstructor
@Slf4j
public class ExternalApiSender implements CommentSender {

    private final CommentClient commentClient;
    private final Scheduler httpScheduler;
    private final CircuitBreakerRegistry circuitBreakerRegistry;

    @CircuitBreaker(name = "commentApiBreaker")
    @Bulkhead(name = "commentApiBulkhead", type = Bulkhead.Type.SEMAPHORE)
    @TimeLimiter(name = "commentApiLimiter")
    @Override
    public CompletableFuture<Void> send(List<CommentEvent> batch) {
        log.info("Попытка отправки пакета размером {}...", batch.size());

        return Mono.fromRunnable(() -> commentClient.sendCommentsBatch(batch))
                .doOnSubscribe(s -> log.info("Асинхронная отправка пакета размером {}...", batch.size()))
                .subscribeOn(httpScheduler)

                .retryWhen(Retry.backoff(3, Duration.ofMillis(500))
                        .filter(this::isRetryableException)
                        .doBeforeRetry(retrySignal ->
                                log.warn("Retry attempt #{}, cause: {}",
                                        retrySignal.totalRetries(),
                                        retrySignal.failure().getMessage())
                        ))

                .transformDeferred(CircuitBreakerOperator.of(circuitBreakerRegistry.circuitBreaker("commentApiBreaker")))
                .timeout(Duration.ofSeconds(5))
                .then()
                .toFuture();
    }

    private boolean isRetryableException(Throwable t) {
        if (t instanceof feign.FeignException feignEx) {
            return feignEx.status() >= 500 || feignEx.status() < 0;
        }
        return true;
    }

    public CompletableFuture<Void> fallbackSend(List<CommentEvent> batch, Throwable t) {
        log.error("Fallback triggered. Причина:", t);
        saveToDeadLetterQueue(batch);
        return CompletableFuture.completedFuture(null); }

    private void saveToDeadLetterQueue(List<CommentEvent> batch) {
        // Логика резервного сохранения (например, запись в файл, в БД otus или в специальный топик-ошибок comments-dlq)
        log.info("Резервное сохранение {} элементов завершено.", batch.size());
    }
}
