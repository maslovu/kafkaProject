package com.maslov.service;

import com.maslov.dto.CommentEvent;
import io.micrometer.core.instrument.Counter;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
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
    @SneakyThrows
    void shouldSendBatchSuccessfully_WhenBufferFillsUpTest() {
        // Arrange — заполняем буфер до лимита (50)
        when(sender.send(anyList())).thenReturn(CompletableFuture.completedFuture(null));

        // Act — отправляем 50 сообщений
        for (int i = 1; i <= 50; i++) {
            CommentEvent event = new CommentEvent((long) i, "Comment " + i);
            ConsumerRecord<String, CommentEvent> record =
                    new ConsumerRecord<>("topic", 0, (long) i, "key" + i, event);
            kafkaListener.listen(record);
        }

        // Assert
        verify(sender, times(1)).send(argThat(list -> list.size() == 50));
        verify(dlqMessagesCounter, never()).increment();
        verify(kafkaTemplate, never()).send(anyString(), any(), any());
    }

    @Test
    @SneakyThrows
    void shouldNotSendWhenBufferNotFullTest() {
        // Arrange
        // Act — отправляем меньше лимита
        ConsumerRecord<String, CommentEvent> record =
                new ConsumerRecord<>("topic", 0, 0L, "key1", validEvent1);
        kafkaListener.listen(record);

        // Assert — батч не отправлен, буфер накопил 1 элемент
        verify(sender, never()).send(anyList());
        verify(dlqMessagesCounter, never()).increment();
    }

    @Test
    @SneakyThrows
    void shouldSendScheduledBatch_WhenFlushBufferScheduledCalledTest() {
        // Arrange
        when(sender.send(anyList())).thenReturn(CompletableFuture.completedFuture(null));

        // Наполняем буфер
        ConsumerRecord<String, CommentEvent> record1 =
                new ConsumerRecord<>("topic", 0, 0L, "key1", validEvent1);
        ConsumerRecord<String, CommentEvent> record2 =
                new ConsumerRecord<>("topic", 0, 1L, "key2", validEvent2);
        kafkaListener.listen(record1);
        kafkaListener.listen(record2);

        // Act — вызываем scheduled сброс
        kafkaListener.flushBufferScheduled();

        // Assert
        verify(sender, times(1)).send(argThat(list -> list.size() == 2));
    }

    @Test
    @SneakyThrows
    void shouldEvacuateToDlt_WhenSenderThrowsExceptionTest() {
        // Arrange
        CommentEvent validEvent = new CommentEvent(1L, "Test comment");
        ConsumerRecord<String, CommentEvent> record =
                new ConsumerRecord<>("comments-topic", 0, 0L, "key1", validEvent);

        java.util.concurrent.CompletableFuture<?> failedFuture =
                new java.util.concurrent.CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("External API is dead"));
        doReturn(failedFuture).when(sender).send(anyList());

        // Act
        kafkaListener.listen(record);
        kafkaListener.flushBufferScheduled();

        // Assert
        verify(dlqMessagesCounter, times(1)).increment(1.0);
        verify(kafkaTemplate, times(1)).send(eq("comments-topic.DLT"), eq("1"), eq(validEvent));
    }
}
