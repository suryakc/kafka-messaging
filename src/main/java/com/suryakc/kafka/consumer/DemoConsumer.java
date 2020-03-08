package com.suryakc.kafka.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class DemoConsumer {

    private final List<String> messages = new ArrayList<>();

    @KafkaListener(topics = "newTopic", groupId = "kafka-sandbox")
    public void listen(String message) {
        synchronized (messages) {
            messages.add(message);
        }
    }

    public List<String> getMessages() {
        return messages;
    }

}