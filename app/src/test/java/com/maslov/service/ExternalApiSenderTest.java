package com.maslov.service;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.maslov.dto.CommentEvent;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.contract.wiremock.AutoConfigureWireMock;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.exactly;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.awaitility.Awaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@SpringBootTest
@AutoConfigureWireMock(port = 0)
@ActiveProfiles("test")
class ExternalApiSenderTest {

    @Autowired
    @MockitoSpyBean
    private ExternalApiSender sender;

    @Autowired
    private CircuitBreakerRegistry registry;

    @Autowired
    private WireMockServer wireMock;

    private CommentEvent event;

    @BeforeEach
    void resetState() {
        event = new CommentEvent();
        event.setComment("Test");
        event.setBookId(1L);

        if (wireMock != null) {
            wireMock.resetRequests(); // Очищаем только счетчики запросов
            wireMock.resetScenarios();
        }

        try {
            CircuitBreaker cb = registry.circuitBreaker("commentApiBreaker");
            cb.transitionToClosedState();

            cb.reset();
        } catch (IllegalArgumentException ignored) {}
    }

    @AfterEach
    void tearDown() {
        try {
            registry.remove("commentApiBreaker");
        } catch (Exception ignored) {}
    }

    @Test
    void shouldSendBatchSuccessfullyTest() throws Exception {
        // Arrange
        stubFor(post(urlEqualTo("/api/books/comment/batch"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("[]")));

        // Act
        CompletableFuture<Void> future = sender.send(List.of(event));

        // Assert - ждем завершения фьючи с таймаутом чуть больше лимита TimeLimiter
        await().atMost(Duration.ofSeconds(5)).until(future::isDone);

        // Проверка метрик CB
        CircuitBreaker cb = registry.circuitBreaker("commentApiBreaker");
        assertThat(cb.getMetrics().getNumberOfSuccessfulCalls()).isEqualTo(1);

        // Проверка логики резервного сохранения (DLQ)
        verify(sender, never()).saveToDeadLetterQueue(anyList());

        // Проверка запроса к WireMock
        com.github.tomakehurst.wiremock.client.WireMock.verify(postRequestedFor(urlEqualTo("/api/books/comment/batch")));
    }

    @Test
    void shouldTriggerFallbackWhenApiIsSlowTest() throws Exception {
        // Arrange
        // Simulate slow downstream service (превышает лимит timeLimiter из yml, который равен 3s)
        stubFor(post(urlEqualTo("/api/books/comment/batch"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withFixedDelay(5000)));

        // Act
        CompletableFuture<Void> future = sender.send(List.of(event));

        // Assert
        await().atMost(Duration.ofSeconds(4)) // Ждем меньше, чем задержка сервера (5 сек), но больше лимита (3 сек)
                .untilAsserted(() -> assertThat(future).isCompleted()); // Проверяем, что вызвался именно fallback внутри нашего спая

        verify(sender).saveToDeadLetterQueue(argThat(list -> list.size() == 1));

        // Метрики должны зафиксировать неудачу
        CircuitBreaker cb = registry.circuitBreaker("commentApiBreaker");

        assertThat(cb.getMetrics().getNumberOfFailedCalls()).isGreaterThanOrEqualTo(1);
    }

    @Test
    @DirtiesContext
    void shouldOpenCircuitBreakerAfterMultipleFailuresTest() throws Exception {
        // Arrange
        // Симулируем жесткую ошибку бэкенда
        // Получаем автомат напрямую
        CircuitBreaker cb = registry.circuitBreaker("commentApiBreaker");

        cb.transitionToOpenState();

        assertThat(cb.getState()).isEqualTo(CircuitBreaker.State.OPEN);

        // Очищаем счетчики вызовов mock-объекта (spy) перед проверочным вызовом
        org.mockito.Mockito.clearInvocations(sender);

        List<CommentEvent> batch = List.of(event);

        // Act - Делаем 4-й вызов при открытом автомате
        CompletableFuture<Void> protectedFuture = sender.send(batch);
        await().atMost(Duration.ofSeconds(2)).until(protectedFuture::isDone);

        // Assert 1: Исключения нет, фьюча завершилась успешно благодаря фолбеку
        assertThat(protectedFuture).isCompleted();

        // Проверяем, что fallback отработал без реального сетевого вызова
        verify(sender).saveToDeadLetterQueue(anyList());

        // ВАЖНО: WireMock НЕ должен получить этот 4-й запрос, так как цепь открыта
        com.github.tomakehurst.wiremock.client.WireMock.verify(
                exactly(0), postRequestedFor(urlEqualTo("/api/books/comment/batch")));
    }

    @Test
    @DirtiesContext
    void shouldTransitionFromHalfOpenToClosedAfterSuccessfulCallsTest() throws Exception {
        // Arrange
        // 1. Создаем кастомную конфигурацию для теста, где жестко фиксируем размер окна в Half-Open
        io.github.resilience4j.circuitbreaker.CircuitBreakerConfig customConfig =
                io.github.resilience4j.circuitbreaker.CircuitBreakerConfig.custom()
                        .failureRateThreshold(50)
                        .slidingWindowSize(20)
                        // ЖЕСТКО ГОВОРИМ: Ждать минимум 5 вызовов в состоянии HALF_OPEN перед тем, как закрыть цепь!
                        .permittedNumberOfCallsInHalfOpenState(5)
                        .build();

        // 2. Пересоздаем автомат в реестре с этой конфигурацией
        registry.remove("commentApiBreaker");
        CircuitBreaker cb = registry.circuitBreaker("commentApiBreaker", customConfig);

        // 3. Выполняем чистый переход по цепочке
        cb.transitionToClosedState();
        cb.reset();
        cb.transitionToOpenState();
        cb.transitionToHalfOpenState();

        assertThat(cb.getState()).isEqualTo(CircuitBreaker.State.HALF_OPEN);

        // Act & Assert
        for (int i = 1; i <= 5; i++) {
            cb.onSuccess(10, TimeUnit.MILLISECONDS);

            // Теперь автомат железно будет оставаться HALF_OPEN на шагах 1, 2, 3, 4
            if (i < 5) {
                assertThat(cb.getState()).isEqualTo(CircuitBreaker.State.HALF_OPEN);
            }
        }

        // Финал: на 5-й успешный вызов автомат перейдет в CLOSED
        assertThat(cb.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
    }
}
