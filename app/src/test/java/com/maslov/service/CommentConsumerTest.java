package com.maslov.service;

import com.maslov.dto.CommentEvent;
import io.micrometer.core.instrument.Counter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CommentConsumerTest {

    @Mock
    private ExternalApiSender sender; // Мокаем наш сервис отправки

    @Mock
    private Counter dlqMessagesCounter;

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    @InjectMocks
    private CommentConsumerService kafkaListener; // Имя вашего класса, где находится @KafkaListener

    private CommentEvent validEvent1;
    private CommentEvent validEvent2;

    @BeforeEach
    void setUp() {
        validEvent1 = new CommentEvent();
        validEvent1.setBookId(1L);
        validEvent1.setComment("First comment");

        validEvent2 = new CommentEvent();
        validEvent2.setBookId(2L);
        validEvent2.setComment("Second comment");
    }

    @Test
    void shouldProcessBatchSuccessfully_WhenAllRecordsAreValid() {
        // Arrange
        ConsumerRecord<String, CommentEvent> record1 = new ConsumerRecord<>("topic", 0, 0L, "key1", validEvent1);
        ConsumerRecord<String, CommentEvent> record2 = new ConsumerRecord<>("topic", 0, 1L, "key2", validEvent2);
        List<ConsumerRecord<String, CommentEvent>> batch = List.of(record1, record2);

        // Настраиваем успешный асинхронный ответ от sender
        when(sender.send(anyList())).thenReturn(CompletableFuture.completedFuture(null));

        // Act
        kafkaListener.listenInBatch(batch);

        // Assert
        // Проверяем, что sender вызван ровно 1 раз со списком из 2 элементов
        verify(sender, times(1)).send(List.of(validEvent1, validEvent2));
    }

    @Test
    void shouldProcessValidRecordsAndIsolateFailedRecordsToDlt_WhenRecordValueThrowsException() {
        // Arrange
        CommentEvent validEvent1 = new CommentEvent(1L, "Valid comment");
        CommentEvent validEvent2 = new CommentEvent(2L, "Error comment");

        ConsumerRecord<String, CommentEvent> record1 = new ConsumerRecord<>("topic", 0, 0L, "key1", validEvent1);

        // Симулируем «битую» запись, которая бросает ошибку при вызове .value()
        ConsumerRecord<String, CommentEvent> spyRecord2 = spy(new ConsumerRecord<>("topic", 0, 1L, "key2", validEvent2));
        doThrow(new RuntimeException("Deserialization error")).when(spyRecord2).value();

        List<ConsumerRecord<String, CommentEvent>> batch = List.of(record1, spyRecord2);

        // Создаем дефолтные выполненные фьючи для заглушек
        java.util.concurrent.CompletableFuture<?> completedFuture =
                java.util.concurrent.CompletableFuture.completedFuture(null);

        // Используем doReturn — он пропустит любые несовпадения типов CompletableFuture в generics
        doReturn(completedFuture).when(kafkaTemplate).send(anyString(), any(), any());
        doReturn(completedFuture).when(sender).send(anyList());

        // Act
        kafkaListener.listenInBatch(batch);

        // Assert
        // 1. Проверяем, что счетчик метрик БЫЛ вызван ровно 1 раз для сбойной записи
        verify(dlqMessagesCounter, times(1)).increment();

        // 2. Проверяем, что сбойная запись была отправлена в DLT руками через kafkaTemplate
        verify(kafkaTemplate, times(1)).send(eq("topic.DLT"), eq("key2"), any());

        // 3. Проверяем, что валидная запись (record1) успешно ушла в бизнес-логику отправителя
        verify(sender, times(1)).send(argThat(list -> list.size() == 1 && list.contains(validEvent1)));
    }

    @Test
    void shouldPropagateException_WhenSenderThrowsException() {
        // Arrange
        ConsumerRecord<String, CommentEvent> record1 = new ConsumerRecord<>("topic", 0, 0L, "key1", validEvent1);
        List<ConsumerRecord<String, CommentEvent>> batch = List.of(record1);

        // Симулируем жесткое падение sender (например, если CircuitBreaker/Retry выбросили ошибку наружу)
        CompletableFuture<Void> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("External API is dead"));

        when(sender.send(anyList())).thenReturn(failedFuture);

        // Act & Assert
        // Метод .join() развернетCompletionException и выбросит исходный RuntimeException наружу в поток Кафки
        assertThatThrownBy(() -> kafkaListener.listenInBatch(batch))
                .hasMessageContaining("External API is dead");

        // Убеждаемся, что попытка отправки была осуществлена
        verify(sender, times(1)).send(anyList());
    }
}
