package com.example;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class SimpleAdmin {
    private final static Logger logger = LoggerFactory.getLogger(SimpleAdmin.class);
    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private final static String GROUP_ID = "test-group";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        AdminClient admin = AdminClient.create(getProperties());
        Collection<Node> nodes = admin.describeCluster().nodes().get();
        logger.info("==GET broker information");
        for (Node node : nodes) {
            logger.info("node={}", node);
            ConfigResource cr = new ConfigResource(ConfigResource.Type.BROKER, node.idString());
            DescribeConfigsResult describeConfigsResult = admin.describeConfigs(Collections.singleton(cr));
            describeConfigsResult.all().get().forEach((broker, config) -> {
                config.entries().forEach(configEntry -> logger.info(configEntry.name() + "= "+configEntry.value()));
            });

            Map<String, TopicDescription> test = admin.describeTopics(Collections.singleton("test")).all().get();
            logger.info("test={}", test);
        }

        admin.close();
    }

    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        return properties;
    }
}
