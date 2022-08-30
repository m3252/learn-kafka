package com.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.Acknowledgment;

@SpringBootApplication
@Slf4j
public class SpringKafkaCommitListenerApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringKafkaCommitListenerApplication.class, args);
	}

	@KafkaListener(topics = "test", groupId = "test-group", containerFactory = "customContainerFactory")
	public void customListener(String data) {
		log.info("data={}", data);
	}
}
