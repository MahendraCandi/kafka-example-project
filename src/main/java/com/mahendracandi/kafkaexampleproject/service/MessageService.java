package com.mahendracandi.kafkaexampleproject.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.lang.Nullable;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class MessageService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public MessageService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(String topicName, Integer totalMessage, @Nullable String message, int messageLength) {
        for (int i = 1; i <= totalMessage; i++){
            final var payload = message != null ? message : RandomStringUtils.randomAlphanumeric(messageLength, messageLength);
            kafkaTemplate.send(
                    MessageBuilder.withPayload(payload)
                            .setHeader(KafkaHeaders.TOPIC, topicName)
                            .setHeader(KafkaHeaders.MESSAGE_KEY, RandomStringUtils.randomAlphanumeric(20, 20))
                            .setHeader(KafkaHeaders.CORRELATION_ID, String.format("AppCorrelationId%s", i))
                            .build()
            ).addCallback(
                    result -> {
                        final var producerRecord = result.getProducerRecord();
                        log.info("key {}: value {}", producerRecord.key(), producerRecord.value());
                    },
                    failed -> {
                        log.error("Failed process message: {}", failed.getMessage());
                    }
            );
        }
    }

}
