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
    private ExternalApiSender sender;

    @Mock
    private Counter dlqMessagesCounter;

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    @InjectMocks
    private CommentConsumerService kafkaListener;

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
        ConsumerRecord<String, CommentEvent> record1 =
                new ConsumerRecord<>("topic", 0, 0L, "key1", validEvent1);
        ConsumerRecord<String, CommentEvent> record2 =
                new ConsumerRecord<>("topic", 0, 1L, "key2", validEvent2);
        List<ConsumerRecord<String, CommentEvent>> batch = List.of(record1, record2);

        when(sender.send(anyList())).thenReturn(CompletableFuture.completedFuture(null));

        // Act
        kafkaListener.listenInBatch(batch);

        // Assert
        verify(sender, times(1)).send(List.of(validEvent1, validEvent2));
    }

    @Test
    void shouldProcessValidRecordsAndIsolateFailedRecordsToDlt_WhenRecordValueThrowsException() {
        // Arrange
        CommentEvent validEvent1 = new CommentEvent(1L, "Valid comment");
        CommentEvent validEvent2 = new CommentEvent(2L, "Error comment");

        ConsumerRecord<String, CommentEvent> record1 =
                new ConsumerRecord<>("topic", 0, 0L, "key1", validEvent1);

        // Симулируем «битую» запись, которая бросает ошибку при вызове .value()
        ConsumerRecord<String, CommentEvent> spyRecord2 =
                spy(new ConsumerRecord<>("topic", 0, 1L, "key2", validEvent2));
        doThrow(new RuntimeException("Deserialization error")).when(spyRecord2).value();

        List<ConsumerRecord<String, CommentEvent>> batch = List.of(record1, spyRecord2);

        // Создаем дефолтные выполненные фьючи для заглушек
        CompletableFuture<?> completedFuture = CompletableFuture.completedFuture(null);

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
        verify(sender, times(1))
                .send(argThat(list -> list.size() == 1 && list.contains(validEvent1)));
    }

    @Test
    void shouldHandleExceptionAndEvacuateToDlt_WhenSenderThrowsException() {
        // Arrange
        CommentEvent validEvent = new CommentEvent(1L, "Test comment");
        ConsumerRecord<String, CommentEvent> record = new ConsumerRecord<>("comments-topic", 0, 0L, "key1", validEvent);
        List<ConsumerRecord<String, CommentEvent>> batch = List.of(record);

        // Заглушка для успешной отправки в DLT
        java.util.concurrent.CompletableFuture<?> kafkaFuture =
                java.util.concurrent.CompletableFuture.completedFuture(null);
        doReturn(kafkaFuture).when(kafkaTemplate).send(anyString(), any(), any());

        // Имитируем падение внешнего API/Сендера
        java.util.concurrent.CompletableFuture<?> failedFuture = new java.util.concurrent.CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("External API is dead"));
        doReturn(failedFuture).when(sender).send(anyList());

        // Act & Assert
        // Мы не оборачиваем вызов в assertThatThrownBy,
        // так как метод поглощает ошибку и возвращает управление без исключений!
        kafkaListener.listenInBatch(batch);

        // Проверяем, что эвакуация в DLT и инкремент метрик сработали корректно
        verify(dlqMessagesCounter, times(1)).increment(eq(1.0));
        verify(kafkaTemplate, times(1)).send(eq("comments-topic.DLT"), eq("1"), eq(validEvent));
        verify(sender, times(1)).send(anyList());
    }

    @Test
    void shouldEvacuateEntireBatchToDltAndIncrementMetrics_WhenSenderFailsGlobally() {
        // Arrange
        CommentEvent event1 = new CommentEvent(1L, "First valid comment");
        CommentEvent event2 = new CommentEvent(2L, "Second valid comment");

        // Создаем два полностью валидных сообщения в батче
        ConsumerRecord<String, CommentEvent> record1 =
                new ConsumerRecord<>("comments-topic", 0, 0L, "key1", event1);
        ConsumerRecord<String, CommentEvent> record2 =
                new ConsumerRecord<>("comments-topic", 0, 1L, "key2", event2);
        List<ConsumerRecord<String, CommentEvent>> batch = List.of(record1, record2);

        // Инициализируем выполненный фьюче для заглушки KafkaTemplate
        java.util.concurrent.CompletableFuture<?> kafkaFuture =
                java.util.concurrent.CompletableFuture.completedFuture(null);
        doReturn(kafkaFuture).when(kafkaTemplate).send(anyString(), any(), any());

        // ГЛАВНЫЙ СТАББИНГ: Заставляем наш sender выбросить ошибку при вызове .join()
        java.util.concurrent.CompletableFuture<?> failedFuture = new java.util.concurrent.CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("Database is down"));
        doReturn(failedFuture).when(sender).send(anyList());

        // Act
        // Метод не должен выбросить исключение наружу, так как блок catch поглощает и обрабатывает ошибку сендера
        kafkaListener.listenInBatch(batch);

        // Assert
        // 1. Проверяем, что счетчик метрик увеличился ровно на РАЗМЕР батча (на 2 элемента)
        verify(dlqMessagesCounter, times(1)).increment(eq(2.0));

        // 2. Проверяем, что КАЖДОЕ сообщение из батча было отправлено в DLT-топик поштучно
        verify(kafkaTemplate, times(1))
                .send(eq("comments-topic.DLT"), eq("1"), eq(event1));
        verify(kafkaTemplate, times(1))
                .send(eq("comments-topic.DLT"), eq("2"), eq(event2));

        // 3. Проверяем, что попытка отправки в основной сендер действительно была совершена
        verify(sender, times(1)).
                send(argThat(list -> list.size() == 2 && list.contains(event1) && list.contains(event2)));
    }
}
