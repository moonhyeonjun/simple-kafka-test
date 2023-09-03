package com.example.kafka_test.controller;

import com.example.kafka_test.service.KafkaTopicService;
import com.example.kafka_test.service.KafkaSampleProducerService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping("/kafka")
public class KafkaSampleProducerController {

    @Autowired
    private KafkaSampleProducerService kafkaSampleProducerService;

    @Autowired
    private KafkaTopicService kafkaTopicService;

    // 메시지를 Kafka 토픽으로 전송하는 엔드포인트입니다.
    @PostMapping("/sendMessage")
    public void sendMessage(@RequestParam String topicName, @RequestParam String message) {
        try {
            kafkaSampleProducerService.sendMessage(topicName, message);
        } catch (Exception e) {
            // 예외가 발생했을 때 처리합니다.
            // 여기에서는 예외를 로깅하고 무시하고 있습니다.
            // 실제로는 예외 처리를 프로젝트 요구 사항에 맞게 수정해야 합니다.
            log.error("Error consuming Kafka message: " + e.getMessage());
        }
    }

    // Kafka 토픽의 파티션 수를 늘리는 엔드포인트입니다.
    @PostMapping("/increasePartitions")
    public void increasePartitions(@RequestParam String topicName, @RequestParam int newPartitionCount) {
        try {
            kafkaTopicService.increaseTopicPartitions(topicName, newPartitionCount);
        }  catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // 인터럽트를 처리하거나 적절한 조치를 취합니다.
            log.error("Thread was interrupted: " + e.getMessage());
        } catch (Exception e) {
            // 예외가 발생했을 때 처리합니다.
            // 여기에서는 예외를 로깅하고 무시하고 있습니다.
            // 실제로는 예외 처리를 프로젝트 요구 사항에 맞게 수정해야 합니다.
            log.error("Error consuming Kafka message: " + e.getMessage());
        }
    }

    // Kafka 토픽의 현재 파티션 수를 가져오는 엔드포인트입니다.
    @PostMapping("/getPartitionCount")
    public int getPartitionCount(@RequestParam String topicName) {
        try {
            return kafkaTopicService.getTopicPartitionCount(topicName);
        } catch (Exception e) {
            // 예외가 발생했을 때 처리합니다. 여기에서는 예외를 적절하게 처리하고 오류 코드나 값(-1)을 반환합니다.
            // 실제로는 예외 처리 방식 및 반환 값을 프로젝트 요구 사항에 맞게 수정해야 합니다.
            return -1; // 오류 상황을 나타내는 값을 반환합니다.
        }
    }
}
