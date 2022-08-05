package com.example.patition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

public class CustomPartitioner implements Partitioner {

    /*
    * 레코드를 기반으로 파티션을 정하는 로직이 들어간다.
    * @return 주어진 레코드가 들어갈 파티션 번호
    * */

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // 메시지 키가 있나요?
        if (keyBytes == null) {
            throw new InvalidRecordException("Need message key");
        }

        // 키가 google 인가요?
        if (((String) key).equals("google")) {
            return 0;
        }

        // 메시지 키를 이용해 해시값을 지정하여 특정 파티션에 매칭
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int size = partitions.size();
        return Utils.toPositive(Utils.murmur2(keyBytes)) % size;

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
