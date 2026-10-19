package com.maslov.configs;

import com.maslov.dto.CommentEvent;
import io.micrometer.core.instrument.Counter;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String servers;

    @Value("${kafka.trusted-packages:com.maslov.dto}")
    private String trustedPackages;

    // Свойства Продюсера (для KafkaTemplate)
    @Bean
    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return props;
    }

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public ConsumerFactory<String, CommentEvent> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        props.put(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, 5000);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);

        //Настраиваем безопасность пакетов JSON
        ErrorHandlingDeserializer<CommentEvent> jsonDeserializer = getObjectJsonDeserializer();

        //Явно передаем StringDeserializer и настроенный JsonDeserializer в фабрику
        return new DefaultKafkaConsumerFactory<>(
                props,
                new StringDeserializer(),
                jsonDeserializer
        );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, CommentEvent> batchFactory(
            ConsumerFactory<String, CommentEvent> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, CommentEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(kafkaConsumerFactory);
        factory.setBatchListener(true); // пакетный режим

        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, CommentEvent> singleFactory(
            ConsumerFactory<String, CommentEvent> kafkaConsumerFactory,
            KafkaTemplate<String, Object> kafkaTemplate,
            Counter dlqMessagesCounter) {
        ConcurrentKafkaListenerContainerFactory<String, CommentEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(kafkaConsumerFactory);
        factory.setBatchListener(false);
        factory.setConcurrency(3);

        // 1. Создаём DLT-рекуперер с инкрементом счётчика
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate) {
            @Override
            public void accept(ConsumerRecord<?, ?> record, Exception exception) {
                dlqMessagesCounter.increment();
                super.accept(record, exception);
            }
        };

        // 2. 2 повтора через 1000мс (всего 3 попытки)
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                recoverer,
                new FixedBackOff(1000L, 2L)
        );

        // 3. Добавляем обработчик в фабрику
        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }

    private ErrorHandlingDeserializer<CommentEvent> getObjectJsonDeserializer() {
        JsonDeserializer<CommentEvent> jsonDeserializer = new JsonDeserializer<>(CommentEvent.class);
        jsonDeserializer.addTrustedPackages(trustedPackages);
        jsonDeserializer.setUseTypeHeaders(false); // Игнорируем заголовки типов продюсера

        // Оборачиваем его в ErrorHandlingDeserializer
        // Это защитит консьюмер от падения при встрече с битым JSON
        return new ErrorHandlingDeserializer<>(jsonDeserializer);
    }

    @Bean
    public NewTopic commentsTopic() {
        return TopicBuilder.name("comments-topic")
                .partitions(3) // Явно говорим создать 3 партиции под concurrency
                .replicas(1)   // Для локальной разработки или теста
                .build();
    }
}
