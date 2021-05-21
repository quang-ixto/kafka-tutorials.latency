package com.example.kafka.latency;

import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;

@EnableKafka
@Component
public class Consumer {

    @KafkaListener(topics = "${kafka.topicName}")
    public void consumer_A(@Payload String message, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key) {
        var elapsed = parseNCalcElapsedTime(message);
        System.out.println("Consumer_A: (key: " + key + ") dt = " + elapsed.toMillis() + "ms -- \t [ " + message + " ]");
    }

    @KafkaListener(topics = "${kafka.topicName}")
    public void consumer_B(@Payload String message, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key) {
        var elapsed = parseNCalcElapsedTime(message);
        System.out.println("Consumer_B: (key: " + key + ") dt = " + elapsed.toMillis() + "ms -- \t [ " + message + " ]");
    }

    @KafkaListener(topics = "${kafka.topicName}")
    public void consumer_C(@Payload String message, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key) {
        var elapsed = parseNCalcElapsedTime(message);
        System.out.println("Consumer_C: (key: " + key + ") dt = " + elapsed.toMillis() + "ms -- \t [ " + message + " ]");
    }


    private Duration parseNCalcElapsedTime(String dateTimeString) {
        var currentDateTime = LocalDateTime.now();
        var receivedDateTime = LocalDateTime.parse(dateTimeString);
        return Duration.between(receivedDateTime, currentDateTime);
    }
}
