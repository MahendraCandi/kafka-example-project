package com.mahendracandi.kafkaexampleproject.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mahendracandi.kafkaexampleproject.service.MessageService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.lang.Nullable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class AppController {

    private final MessageService messageService;
    private final ObjectMapper objectMapper;

    public AppController(MessageService messageService, ObjectMapper objectMapper) {
        this.messageService = messageService;
        this.objectMapper = objectMapper;
    }

    @GetMapping("send-message")
    public ResponseEntity<String> sendMessage(
            @RequestParam("topic-name") String topicName,
            @RequestParam("total-message") Integer totalMessage,
            @RequestParam(value = "random-message-length", required = false, defaultValue = "100") int messageLength,
            @Nullable @RequestBody(required = false) Map<String, Object> requestBodyAsJson
            ) throws JsonProcessingException {
        final var message = requestBodyAsJson != null ? objectMapper.writeValueAsString(requestBodyAsJson) : null;
        messageService.send(topicName, totalMessage, message, messageLength);
        return new ResponseEntity<>(
                String.format("%s message has been sent to topic %s", totalMessage, topicName),
                HttpStatus.OK);
    }
}
