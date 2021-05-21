package com.example.kafka.latency;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@EnableScheduling
@Component
public class Producer {

    @Value("${kafka.topicName}")
    private String topicName;

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @Scheduled(fixedRate = 1300)
    public void sendMessages() {
        var currentTime = LocalDateTime.now();
        var key = computeKey(currentTime);
        kafkaTemplate.send(topicName, key, currentTime.toString());
    }

    private String computeKey(LocalDateTime dateTime) {
        return dateTime.getSecond() % 3 == 0 ?
                    "% 3" :
                dateTime.getSecond() % 2 == 0 ?
                    "% 2" :
                    "% 1";
    }

}

