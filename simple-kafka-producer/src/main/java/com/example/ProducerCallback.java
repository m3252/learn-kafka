package com.example;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerCallback implements Callback {

    private final static Logger logger = LoggerFactory.getLogger(ProducerCallback.class);


    // 비동기 결과를 받기 위해 사용한다.
    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            logger.error(exception.getMessage());
        } else {
            logger.info("metadata={}", metadata.toString());
        }
    }
}
