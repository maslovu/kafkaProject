package com.maslov.configs;

import com.maslov.dto.CommentEvent;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@SpringBootTest
class KafkaBatchConsumerConfigTest {

    @Autowired
    private ConsumerFactory<String, CommentEvent> consumerFactory;

    @Test
    void consumerFactoryShouldUseErrorHandlingDeserializerTest() {
        DefaultKafkaConsumerFactory<String, CommentEvent> factory =
                (DefaultKafkaConsumerFactory<String, CommentEvent>) consumerFactory;

        Deserializer<CommentEvent> deserializer = factory.getValueDeserializer();
        assertThat(deserializer).isInstanceOf(ErrorHandlingDeserializer.class);
    }
}
