package com.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class SimpleProducer {
    private final static Logger logger = LoggerFactory.getLogger(SimpleProducer.class);
    private final static String TOPIC_NAME = "test";

    //전송할 카프카 클러스터 정보
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";

    public static void main(String[] args) {
        //1.필수 옵션과 선택 옵션을 설정한다.
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //2.설정 값을 프로듀서에 인자로 넘긴다.
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        String messageValue = "testMessage";
        //3.카프카 브로커로 데이터를 보내기 위해 프로듀서 레코드를 생성한다.
        //메시지 키를 담지 않았기 때문에 null로 설정된다.
        //ProducerRecord(String topic, Integer partition, Long timestamp, K key, V value, Iterable<Header> headers)
//        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, messageValue); // key null
//        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "google", "gmail"); // key google
        int partitionNo = 0;
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, partitionNo,"google", "gmail"); //파티션 지정

        //즉각 전송이 아닌 레코드를 프로듀서 내부에 가지고 있다가 배치 형태로 묶어서 브로커에 전송한다.
        producer.send(record);
        logger.info("{}", record);
        //프로듀서 내부 버퍼에 가지고 있는 레코드 목록을 브로커로 전송한다.
        producer.flush();
        //프로듀서 리소스 종료
        producer.close();
    }
}
