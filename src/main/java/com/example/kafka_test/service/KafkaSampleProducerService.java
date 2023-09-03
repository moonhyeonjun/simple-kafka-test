package com.example.kafka_test.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class KafkaSampleProducerService {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    // Kafka 메시지를 생성하고 특정 토픽으로 전송하는 서비스 클래스입니다.
    public void sendMessage(String topicName, String message) {
        log.info("send message : " +  message);

        try {
            // Determine the producer's ID dynamically (e.g., from configuration or a context variable).
            String producerId = "Producer1";

            // Kafka 템플릿을 사용하여 메시지를 지정된 토픽으로 전송합니다.
            // Producer ID를 헤더로 설정
            Message<String> kafkaMessage = MessageBuilder
                    .withPayload(message)
                    .setHeader(KafkaHeaders.TOPIC, topicName)
                    .setHeader("producerId", producerId)
                    .build();

            kafkaTemplate.send(kafkaMessage);
        } catch (Exception e) {
            // 예외가 발생할 수 있는 경우를 고려하여 예외 처리를 수행하는 것이 좋습니다.
            // 여기에서는 예외를 로깅하고 무시하고 있습니다.
            // 실제로는 예외를 적절하게 처리하고 로깅하는 방식을 선택해야 합니다.
            log.error("Error sending message to Kafka: " + e.getMessage());
        }
    }
}
