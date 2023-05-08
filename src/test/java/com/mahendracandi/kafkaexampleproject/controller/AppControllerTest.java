package com.mahendracandi.kafkaexampleproject.controller;

import io.restassured.RestAssured;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.concurrent.LinkedBlockingDeque;

import static io.restassured.RestAssured.given;

@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(
        bootstrapServersProperty = "spring.kafka.bootstrap-servers",
        topics = "unit-test-topic",
        brokerProperties = {
                "listeners=PLAINTEXT://localhost:8183",
                "port=8183"
        }
)
class AppControllerTest {

    public static final String UNIT_TEST_TOPIC = "unit-test-topic";

    @LocalServerPort
    private Integer port;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker; // ignore Intellij error if any! the autowired should work correctly.
    private LinkedBlockingDeque<ConsumerRecord<String, String>> records;
    private KafkaMessageListenerContainer<String, String> container;

    @BeforeEach
    void setUp() {
        RestAssured.port = this.port;

        final var consumerProps = KafkaTestUtils.consumerProps("consumer", "false", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        final var consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps);
        final var containerProperties = new ContainerProperties(UNIT_TEST_TOPIC);
        container = new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);

        records = new LinkedBlockingDeque<>();
        container.setupMessageListener((MessageListener<String, String>) records::add);
        container.start();

        ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());
    }

    @AfterEach
    void tearDown() {
        container.stop();
    }

    @Test
    void shouldSuccessSendRandomMessageToKafka() {
        final var totalMessage = 5;
        final var lengthMessage = 10;
        given()
                .param("total-message", totalMessage)
                .param("random-message-length", lengthMessage)
                .get("/send-random-message/{topic-name}", UNIT_TEST_TOPIC)
                .then()
                .assertThat()
                .log().all()
                .statusCode(HttpStatus.OK.value())
                .body(Matchers.equalTo(String.format("%s message has been sent to topic %s", totalMessage, UNIT_TEST_TOPIC)));

        Awaitility.await()
                .untilAsserted(() -> {
                    Assertions.assertThat(records).isNotNull().hasSize(totalMessage);

                    records.forEach(payload -> {
                        Assertions.assertThat(payload.value().length()).isEqualTo(lengthMessage);
                    });
                });
    }

    @Test
    void shouldSuccessSendRequestBodyToKafka() {
        final var requestBody = "{\"value\":\"test request body\"}";
        given()
                .contentType("application/json")
                .body(requestBody)
                .post("/send-body/{topic-name}", UNIT_TEST_TOPIC)
                .then()
                .assertThat()
                .log().all()
                .statusCode(HttpStatus.OK.value())
                .body(Matchers.equalTo("Payload has been sent"));

        Awaitility.await()
                .untilAsserted(() -> {
                    Assertions.assertThat(records).isNotNull().hasSize(1);

                    records.forEach(payload -> {
                        Assertions.assertThat(payload.value()).isEqualTo(requestBody);
                    });
                });
    }
}
