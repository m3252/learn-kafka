package com.example;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class SimpleKafkaProcessor {
    private final static Logger logger = LoggerFactory.getLogger(SimpleKafkaProcessor.class);

    private final static String APPLICATION_NAME = "processor-application";
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private final static String STREAM_LOG = "stream_log";
    private final static String STREAM_LOG_FILTER = "stream_log_filter";

    public static void main(String[] args) {
        Topology topology = new Topology();
        topology.addSource("Source", STREAM_LOG)
                .addProcessor("Processor", FilterProcessor::new, "Source")
                .addSink("Sink", STREAM_LOG_FILTER, "Processor");

        KafkaStreams streaming = new KafkaStreams(topology, getProperties());
        streaming.start();
    }

    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return properties;
    }
}
