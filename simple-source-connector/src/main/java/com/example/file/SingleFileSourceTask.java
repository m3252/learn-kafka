package com.example.file;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SingleFileSourceTask extends SourceTask {

    private Logger log = LoggerFactory.getLogger(SingleFileSourceTask.class);

    public final String FILENAME_FILED = "filename";

    public final String POSITION_FIELD = "position";

    private Map<String, String> fileNamePartition;

    private Map<String, Object> offset;

    private String topic;

    private String file;

    private long position = -1;

    @Override
    public String version() {
        return "v1.0";
    }

    @Override
    public void start(Map<String, String> props) {

        try {
            // 변수 선언
            SingleFileSourceConnectorConfig config = new SingleFileSourceConnectorConfig(props);

            topic = config.getString(SingleFileSourceConnectorConfig.TOPIC_NAME);
            file = config.getString(SingleFileSourceConnectorConfig.DIR_FILE_NAME);
            fileNamePartition = Collections.singletonMap(FILENAME_FILED, file);
            offset = context.offsetStorageReader().offset(fileNamePartition);

            // offsetStorageReader 에서 파일 오프셋 가져오기
            if (offset != null) {
                Object lastReadFileOffset = offset.get(POSITION_FIELD);
                if (lastReadFileOffset != null) {
                    position = (Long) lastReadFileOffset;
                }
            } else {
                position = 0L;
            }
        } catch (Exception e) {
            throw new ConfigException(e.getMessage(), e);
        }

    }

    @Override
    public List<SourceRecord> poll() {
        List<SourceRecord> records = new ArrayList<>();
        try {
            Thread.sleep(1000L);
            List<String> lines = getLines(position);

            if (lines.size() > 0) {
                lines.forEach(line -> {
                    Map<String, Long> sourceOffset = Collections.singletonMap(POSITION_FIELD, ++position);
                    SourceRecord sourceRecord = new SourceRecord(fileNamePartition, sourceOffset, topic, Schema.STRING_SCHEMA, line);
                    records.add(sourceRecord);
                });
            }
            return records;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private List<String> getLines(long position) throws Exception{
        BufferedReader reader = Files.newBufferedReader(Paths.get(file));
        return reader.lines().skip(position).collect(Collectors.toList());
    }

    @Override
    public void stop() {

    }
}
