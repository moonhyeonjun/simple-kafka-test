package com.example.kafka_test.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Slf4j
@Service
public class KafkaSampleConsumerService {

    // Kafka 토픽 "topic1"을 구독하는 컨슈머 서비스 클래스입니다.
    // "group-id-1" 그룹 ID를 사용하여 여러 컨슈머를 그룹화할 수 있습니다.
    @KafkaListener(topics = "topic1", groupId = "group-id-1")
    public void consumeTopic1(String message, @Header("producerId") String producerId, ConsumerRecord<String, String> kafkaRecord) throws IOException {
        try {
            // Kafka 메시지를 소비하고 처리하는 로직을 구현합니다.
            // 메시지 헤더에서 프로세서 ID를 추출할 수 있습니다.
            log.info("Received message: " + message);
            // processorId가 있는지 확인합니다.
            if (producerId != null && !producerId.isEmpty()) {
                log.info("producerId ID: " + producerId);
            } else {
                log.info("No producerId ID available.");
            }
            log.info("Partition: " + kafkaRecord.partition());
            log.info("Offset: " + kafkaRecord.offset());
        } catch (Exception e) {
            // 예외가 발생했을 때 처리합니다.
            // 여기에서는 예외를 로깅하고 무시하고 있습니다.
            // 실제로는 예외 처리를 프로젝트 요구 사항에 맞게 수정해야 합니다.
            log.error("Error consuming Kafka message for topic1: " + e.getMessage());
        }
    }

    // Kafka 토픽 "topic2"을 구독하는 컨슈머 서비스 클래스입니다.
    // "group-id-2" 그룹 ID를 사용하여 여러 컨슈머를 그룹화할 수 있습니다.
    @KafkaListener(topics = "topic2", groupId = "group-id-2")
    public void consumeTopi2(String message, @Header("producerId") String producerId, ConsumerRecord<String, String> kafkaRecord) throws IOException {
        try {
            // Kafka 메시지를 소비하고 처리하는 로직을 구현합니다.
            // 메시지 헤더에서 프로세서 ID를 추출할 수 있습니다.
            log.info("Received message: " + message);
            // processorId가 있는지 확인합니다.
            if (producerId != null && !producerId.isEmpty()) {
                log.info("producerId ID: " + producerId);
            } else {
                log.info("No producerId ID available.");
            }
            log.info("Partition: " + kafkaRecord.partition());
            log.info("Offset: " + kafkaRecord.offset());
        } catch (Exception e) {
            // 예외가 발생했을 때 처리합니다.
            // 여기에서는 예외를 로깅하고 무시하고 있습니다.
            // 실제로는 예외 처리를 프로젝트 요구 사항에 맞게 수정해야 합니다.
            log.error("Error consuming Kafka message for topic1: " + e.getMessage());
        }
    }

    // Kafka 토픽 "topic3"을 구독하는 컨슈머 서비스 클래스입니다.
    // "group-id-2" 그룹 ID를 사용하여 여러 컨슈머를 그룹화할 수 있습니다.
    // kafka-console-producer --topic topic3 --bootstrap-server localhost:9092
    @KafkaListener(topics = "topic3", groupId = "group-id-3")
    public void consumeTopi3(String message, ConsumerRecord<String, String> kafkaRecord) throws IOException {
        try {
            // Kafka 메시지를 소비하고 처리하는 로직을 구현합니다.
            log.info("Received message: " + message);
            log.info("Partition: " + kafkaRecord.partition());
            log.info("Offset: " + kafkaRecord.offset());
        } catch (Exception e) {
            // 예외가 발생했을 때 처리합니다.
            // 여기에서는 예외를 로깅하고 무시하고 있습니다.
            // 실제로는 예외 처리를 프로젝트 요구 사항에 맞게 수정해야 합니다.
            log.error("Error consuming Kafka message for topic1: " + e.getMessage());
        }
    }
}
