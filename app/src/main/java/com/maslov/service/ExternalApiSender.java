package com.maslov.service;

import com.maslov.client.CommentClient;
import com.maslov.dto.CommentEvent;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class ExternalApiSender {

    private final CommentClient commentClient;

    @Retry(name = "commentApiRetry", fallbackMethod = "fallbackSend")
    @CircuitBreaker(name = "commentApiBreaker")
    public void send(List<CommentEvent> batch) {
        log.info("Попытка отправки пакета размером {}...", batch.size());
        commentClient.sendCommentsBatch(batch);
        log.info("Пакет успешно доставлен.");
    }
}
