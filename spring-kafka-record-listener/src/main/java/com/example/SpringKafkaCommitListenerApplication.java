package com.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;

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
	@KafkaListener(topics = "test", groupId = "test-group-00")
	public void recordListener(ConsumerRecord<String, String> record) {
		log.info("record={}", record);
	}

	/**
	 * 메시지 값에 대한 처리를 수행할 수 있다.
	 */
	@KafkaListener(topics = "test", groupId = "test-group-01")
	public void singleTopicListener(String messageValue) {
		log.info("messageValue={}", messageValue);
	}

	/**
	 * 개별 리스너에 카프카 컨슈머 옵션값을 properties 옵션을 통해 부여할 수 있다.
	 */
	@KafkaListener(topics = "test", groupId = "test-group-02", properties = {
			"max.poll.lnterval.ms:60000",
			"auto.offset.reset:earliest"
	})
	public void singleTopicWithPropertiesListener(String messageValue) {
		log.info("messageValue={}", messageValue);
	}

	/**
	 * 2개 이상의 카프카 컨슈머 스레드를 실행하고 싶다면 concurrency 옵션을 사용한다.
	 */
	@KafkaListener(topics = "test", groupId = "test-group-3", concurrency = "3")
	public void concurrentTopicListener(String messageValue) {
		log.info("messageValue={}", messageValue);
	}

	/**
	 * 특정 토픽의 특정 파티션만 구독하고 싶다면 topicPartitions 파라미터를 사용한다.
	 * 여기에 추가로 @PartitionOffset 을 활용하면 특정 파티션에 특정 오프셋까지 지정 가능하다.
	 * 이 경우 그룹 아이디와 관계없이 항상 설정한 오프셋의 데이터부터 가져온다.
	 */
	@KafkaListener(topicPartitions = {
			@TopicPartition(topic = "test01", partitions = {"0", "1"}),
			@TopicPartition(topic = "test02"
					, partitionOffsets = @PartitionOffset(partition = "0", initialOffset = "3")
			)
	},
	groupId = "test-group-04")
	public void listenSpecificPartition(ConsumerRecord<String, String> record) {
		log.info("record={}", record);
	}


}
