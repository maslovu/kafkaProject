package com.maslov.service;

import com.maslov.dto.CommentEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.listener.BatchListenerFailedException;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyList;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CommentConsumerTest {

    @Mock
    private ExternalApiSender sender; // Мокаем наш сервис отправки

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
    void shouldThrowBatchListenerFailedException_WhenRecordValueThrowsException() {
        // Arrange
        ConsumerRecord<String, CommentEvent> record1 = new ConsumerRecord<>("topic", 0, 0L, "key1", validEvent1);

        // Симулируем «битую» запись, которая при вызове .value() бросает ошибку (например, сбой десериализации)
        ConsumerRecord<String, CommentEvent> spyRecord2 = spy(new ConsumerRecord<>("topic", 0, 1L, "key2", validEvent2));
        doThrow(new RuntimeException("Deserialization error")).when(spyRecord2).value();

        List<ConsumerRecord<String, CommentEvent>> batch = List.of(record1, spyRecord2);

        // Act & Assert
        // Метод должен упасть на 2-м элементе (индекс 1) и выбросить BatchListenerFailedException
        assertThatThrownBy(() -> kafkaListener.listenInBatch(batch))
                .isInstanceOf(BatchListenerFailedException.class)
                .hasMessageContaining("Error processing record in batch")
                .hasFieldOrPropertyWithValue("index", 1); // Проверяем, что Spring Kafka получит правильный индекс ошибки!

        // Так как метод упал в цикле, до отправки дело вообще не должно дойти
        verify(sender, never()).send(anyList());
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