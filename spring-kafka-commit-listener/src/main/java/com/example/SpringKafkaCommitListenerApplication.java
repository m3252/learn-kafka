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

	/**
	 * ack-mode 옵션 값이 MANUAL 또는 MANUAL_IMMEDIATE 라면 수동 커밋을 사용하기 위해 Acknowledgment 인스턴스를 받아야 한다.
	 * Acknowledgment.acknowledge() 메서드 호출을 통해 수동 커밋할 수 있다.
	 */
	@KafkaListener(topics = "test", groupId = "test-group-01")
	public void commitListener(ConsumerRecords<String, String> records, Acknowledgment ack) {
		records.forEach(record -> log.info("record={}", record));
		ack.acknowledge();
	}

	/**
	 * 동기 커밋, 비동기 커밋을 사용하고 싶다면 컨슈머 인스턴스를 사용할 수 있다.
	 * commitAsync(), commitSync() 를 통해 원하는 타이밍에 커밋할 수 있다.
	 * 다만, 리스너가 커밋을 하지 않도록 ack-mode 옵션 값을 MANUAL 또는 MANUAL_IMMEDIATE 설정이 필요하다.
	 */
	@KafkaListener(topics = "test", groupId = "test-group-02")
	public void consumerCommitListener(ConsumerRecords<String, String> records, Consumer<String, String> consumer) {
		records.forEach(record -> log.info("record={}", record));
		consumer.commitAsync();
		consumer.commitSync();
	}
}
