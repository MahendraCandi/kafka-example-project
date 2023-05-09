package com.mahendracandi.kafkaexampleproject.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@Slf4j
public class MessageService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public MessageService(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public void sendRandomMessage(String topicName, Integer totalMessage, int messageLength, boolean isUseKey) {
        for (int i = 1; i <= totalMessage; i++){
            final var payload = RandomStringUtils.randomAlphanumeric(messageLength, messageLength);
            send(buildGenericMessage(String.format("%s-%s", i, payload), topicName, String.format("AppCorrelationId%s", i), isUseKey));
        }
    }

    public void sendBody(Map<String, Object> requestBodyAsJson, String topicName) throws JsonProcessingException {
        final var payload = objectMapper.writeValueAsString(requestBodyAsJson);
        send(buildGenericMessage(payload, topicName, RandomStringUtils.randomAlphanumeric(5, 5), true));
    }

    private void send(Message<String> genericMessage) {
        kafkaTemplate.send(genericMessage)
                .addCallback(
                        result -> {
                            final var producerRecord = result.getProducerRecord();
                            log.info("key {}: value {}", producerRecord.key(), producerRecord.value());
                        },
                        failed -> {
                            log.error("Failed process message: {}", failed.getMessage());
                        }
                );
    }

    private static Message<String> buildGenericMessage(String payload, String topicName, String correlationId, boolean isUseKey) {
        final var messageBuilder = MessageBuilder.withPayload(payload)
                .setHeader(KafkaHeaders.TOPIC, topicName)
                .setHeader(KafkaHeaders.CORRELATION_ID, correlationId);

        if (isUseKey)
            messageBuilder
                .setHeader(KafkaHeaders.MESSAGE_KEY, RandomStringUtils.randomAlphanumeric(20, 20));

        return messageBuilder
                .build();
    }
}
