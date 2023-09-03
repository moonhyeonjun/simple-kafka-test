package com.example.kafka_test.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Slf4j
@Service
public class KafkaTopicService {

    @Autowired
    private KafkaAdmin kafkaAdmin;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    // Kafka 토픽의 파티션 수를 증가시키는 메서드입니다.
    public void increaseTopicPartitions(String topicName, int newPartitionCount) throws ExecutionException, InterruptedException {
        AdminClient adminClient = null;
        try {
            adminClient = AdminClient.create(getAdminConfig());
            // 토픽이 이미 존재하는지 확인합니다.
            if (!adminClient.listTopics().names().get().contains(topicName)) {
                // 토픽이 존재하지 않으면 원하는 파티션 수로 새로운 토픽을 생성합니다.
                NewTopic newTopic = new NewTopic(topicName, newPartitionCount, (short) 1);
                adminClient.createTopics(Collections.singletonList(newTopic));
            } else {
                // 토픽이 이미 존재하는 경우 파티션 수를 증가시킵니다.
                Map<String, NewPartitions> newPartitions = new HashMap<>();
                newPartitions.put(topicName, NewPartitions.increaseTo(newPartitionCount));
                adminClient.createPartitions(newPartitions).all().get();
            }
        } finally {
            if (adminClient != null) {
                // "finally" 절에서 "AdminClient" 객체를 닫아줍니다.
                adminClient.close();
            }
        }
    }

    // Kafka 토픽의 현재 파티션 수를 가져오는 메서드입니다.
    public int getTopicPartitionCount(String topicName) {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            DescribeTopicsResult topicsResult = adminClient.describeTopics(Collections.singletonList(topicName));
            Map<String, KafkaFuture<TopicDescription>> topicDescriptionMap = topicsResult.values();
            KafkaFuture<TopicDescription> topicDescriptionFuture = topicDescriptionMap.get(topicName);

            TopicDescription topicDescription = topicDescriptionFuture.get();
            return topicDescription.partitions().size();
        }catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // 인터럽트를 처리하거나 적절한 조치를 취합니다.
            return -1; // 오류 상황을 나타내는 값을 반환합니다.
        } catch (Exception e) {
            // 예외가 발생했을 때 처리합니다. 여기에서는 예외를 적절하게 처리하고 오류 코드나 값(-1)을 반환합니다.
            // 실제로는 예외 처리 방식 및 반환 값을 프로젝트 요구 사항에 맞게 수정해야 합니다.
            return -1; // 오류 상황을 나타내는 값을 반환합니다.
        }
    }

    // AdminClient 설정을 가져오는 보조 메서드입니다.
    private Map<String, Object> getAdminConfig() {
        Map<String, Object> adminConfig = new HashMap<>();
        adminConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAdmin.getConfigurationProperties().get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG));
        return adminConfig;
    }
}
