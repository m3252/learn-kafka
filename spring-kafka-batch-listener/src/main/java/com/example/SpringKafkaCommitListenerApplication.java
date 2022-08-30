package com.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;

import java.util.List;

@SpringBootApplication
@Slf4j
public class SpringKafkaCommitListenerApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringKafkaCommitListenerApplication.class, args);
	}

	/**
	 * 가장 기본적인 리스너 선언
	 * poll()이 호출되어 가져온 레코드들은 차례대로 개별 레코드의 메시지 값을 파라미터로 받게 된다.
	 * 파라미터로 컨슈머 레코드를 받기 때문에 메시지 키, 메시지 값에 대한 처리를 수행할 수 있다.
	 */
	@KafkaListener(topics = "test", groupId = "test-group-01")
	public void batchListener(ConsumerRecords<String, String> records) {
		records.forEach(record -> {
			log.info("record={}", record);
		});
	}

	@KafkaListener(topics = "test", groupId = "test-group-02")
	public void batchListener(List<String> values) {
		values.forEach(recordValue -> {
			log.info("recordValue={}", recordValue);
		});
	}

	@KafkaListener(topics = "test", groupId = "test-group-02", concurrency = "3")
	public void concurrentBatchListener(ConsumerRecords<String, String> records) {
		records.forEach(record -> {
			log.info("record={}", record);
		});
	}


}
