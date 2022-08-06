package com.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class SimpleConsumer {
    private final static Logger logger = LoggerFactory.getLogger(SimpleConsumer.class);
    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private final static String GROUP_ID = "test-group";

    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getProperties());
        consumer.subscribe(Arrays.asList(TOPIC_NAME));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
//            Map<TopicPartition, OffsetAndMetadata> currentOffset = new HashMap<>();
            for (ConsumerRecord<String, String> record : records) {
                logger.info("record={}", record);
//                currentOffset.put(
//                        new TopicPartition(record.topic(), record.partition()),
//                        new OffsetAndMetadata(record.offset() + 1, null)
//                );

            }
            //동기 오프셋 커밋
            // currentOffset이 없다면 poll()로 반환된 가장 마지막 레코드의 오프셋 기준이며, 개별 레코드 단위로 매번 오프셋을 커밋하고 싶다면 currentOffset 사용
//            consumer.commitSync(currentOffset);

            consumer.commitAsync(
                    (offsets, exception) -> {
                        if (exception != null) {
                            logger.error("commit failed");
                        } else {
                            logger.info("commit succeeded");
                        }
                        if (exception != null) {
                            logger.error("commit failed for offsets {}", offsets, exception);
                        }
                    }
            );
        }
    }

    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 동기 오프셋 커밋 옵션
        //properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        //비동기 오프셋 커밋 옵션
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return properties;
    }
}
