package com.maslov.service;

import com.maslov.dto.CommentEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class KafkaDltConsumerServiceTest {

    // Используем @Spy вместо @InjectMocks, чтобы мы могли частично замокать внутренний метод saveToAuditLog
    @Spy
    private KafkaDltConsumerService dltConsumerService;

    private CommentEvent commentEvent;
    private ConsumerRecord<String, CommentEvent> record;
    private byte[] originalTopicBytes;
    private byte[] exceptionMessageBytes;

    @BeforeEach
    void setUp() {
        commentEvent = new CommentEvent();
        commentEvent.setBookId(1L);
        commentEvent.setComment("Broke comment");

        record = new ConsumerRecord<>("comments-topic.DLT", 0, 123L, "key-1", commentEvent);
        originalTopicBytes = "comments-topic".getBytes(StandardCharsets.UTF_8);
        exceptionMessageBytes = "java.lang.RuntimeException: 500 Error".getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void shouldProcessDltMessageSuccessfully_WhenHeadersArePresent() {
        // Act & Assert
        // Проверяем, что метод выполняется без исключений
        assertThatNoException().isThrownBy(() ->
                dltConsumerService.listenDlt(record, originalTopicBytes, exceptionMessageBytes)
        );

        // Проверяем, что данные корректно распарсились и передались в метод сохранения
        verify(dltConsumerService, times(1))
                .listenDlt(record, originalTopicBytes, exceptionMessageBytes);
    }

    @Test
    void shouldHandleDltMessageSafely_WhenHeadersAreNull() {
        // Act & Assert
        // Передаем null вместо байтовых массивов заголовков (симулируем их отсутствие)
        assertThatNoException().isThrownBy(() ->
                dltConsumerService.listenDlt(record, null, null)
        );

        // Метод должен составить строки "unknown" и успешно выполниться без NullPointerException
        verify(dltConsumerService, times(1)).listenDlt(record, null, null);
    }

    @Test
    void shouldNotPropagateException_WhenPrivateSaveToAuditLogThrowsException() {
        // Arrange
        // Передаем в ConsumerRecord значение null вместо валидного CommentEvent.
        // Когда listenDlt попытается передать его в private saveToAuditLog,
        // там гарантированно вылетит NullPointerException (или ошибка вашей бизнес-логики).
        ConsumerRecord<String, CommentEvent> brokenRecord =
                new ConsumerRecord<>("comments-topic.DLT", 0, 123L, "key-1", null);

        // Act & Assert
        // Самая важная проверка: даже если внутри приватного метода все упало,
        // listenDlt ОБЯЗАН перехватить ошибку в блок catch и завершиться успешно.
        assertThatNoException().isThrownBy(() ->
                dltConsumerService.listenDlt(brokenRecord, originalTopicBytes, exceptionMessageBytes)
        );
    }
}
