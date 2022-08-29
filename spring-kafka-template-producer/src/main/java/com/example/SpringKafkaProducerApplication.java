package com.example;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaProducerException;
import org.springframework.kafka.core.KafkaSendCallback;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

@Slf4j
@SpringBootApplication
public class SpringKafkaProducerApplication implements CommandLineRunner {

    private static String TOPIC_NAME = "test";

    @Autowired
    private KafkaTemplate<String, String> customTemplate;

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(SpringKafkaProducerApplication.class);
        application.run(args);

    }

    @Override
    public void run(String... args) throws InterruptedException {
        ListenableFuture<SendResult<String, String>> future = customTemplate.send(TOPIC_NAME, "test");

        future.addCallback(new KafkaSendCallback<>(){

            @Override
            public void onSuccess(SendResult<String, String> result) {
                log.info("result={}", result);
            }

            @Override
            public void onFailure(KafkaProducerException ex) {
                log.info("ex", ex);
            }
        });
    }
}
