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
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@Service
@RequiredArgsConstructor
@Slf4j
public class ExternalApiSender implements CommentSender {

    private final CommentClient commentClient;
    private final Scheduler httpScheduler;

    @Override
    @Retry(name = "commentApiRetry", fallbackMethod = "fallbackSend")
    @CircuitBreaker(name = "commentApiBreaker")
    @Bulkhead(name = "commentApiBulkhead", type = Bulkhead.Type.SEMAPHORE)
    @TimeLimiter(name = "commentApiLimiter", fallbackMethod = "fallbackSend")
    public CompletableFuture<Void> send(List<CommentEvent> batch) {
        log.info("Асинхронная отправка пакета размером {}...", batch.size());
        Mono<Void> mono = Mono.fromRunnable(() -> commentClient.sendCommentsBatch(batch))
                .then()
                .doOnSuccess(v -> log.info("Пакет успешно доставлен."))
                .doOnError(e -> log.error("Ошибка при доставке.", e))
                .subscribeOn(httpScheduler); // Запускаем именно здесь
        return mono.toFuture();
    }

    public CompletableFuture<Void> fallbackSend(List<CommentEvent> batch, Throwable t) {
        log.error("Fallback triggered. Причина:", t);
        // НЕ бросаем Exception! Даем Кафке закоммитить офсет, чтобы не заблокировать поток.
        try {
            saveToDeadLetterQueue(batch);
        } catch (Exception dlqEx) {
            log.error("CRITICAL: DLQ тоже упал! Сообщения могут быть потеряны.", dlqEx);
            // Здесь можно рассмотреть повторную попытку записи в БД или отправку алерта
        }
        return CompletableFuture.completedFuture(null);
    }

    private void saveToDeadLetterQueue(List<CommentEvent> batch) {
        // Логика резервного сохранения (например, запись в файл, в БД otus или в специальный топик-ошибок comments-dlq)
        log.info("Резервное сохранение {} элементов завершено.", batch.size());
    }
}
