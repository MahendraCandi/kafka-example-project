package com.mahendracandi.kafkaexampleproject.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mahendracandi.kafkaexampleproject.service.MessageService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.lang.Nullable;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
public class AppController {

    private final MessageService messageService;

    public AppController(MessageService messageService) {
        this.messageService = messageService;
    }

    @GetMapping("send-random-message/{topic-name}")
    public ResponseEntity<String> sendMessage(
            @PathVariable("topic-name") String topicName,
            @RequestParam("total-message") Integer totalMessage,
            @RequestParam(value = "random-message-length", required = false, defaultValue = "100") int messageLength
    ) {
        messageService.sendRandomMessage(topicName, totalMessage, messageLength);
        return new ResponseEntity<>(
                String.format("%s message has been sent to topic %s", totalMessage, topicName),
                HttpStatus.OK);
    }

    @PostMapping("send-body/{topic-name}")
    public ResponseEntity<String> sendBody(
            @PathVariable("topic-name") String topicName,
            @Nullable @RequestBody(required = false) Map<String, Object> requestBodyAsJson
    ) throws JsonProcessingException {
        messageService.sendBody(requestBodyAsJson, topicName);
        return new ResponseEntity<>("Payload has been sent", HttpStatus.OK);
    }
}
