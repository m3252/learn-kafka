package com.example.file;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;

public class SingleFileSinkConnectorConfig extends AbstractConfig {

    public static final String DIR_FILE_NAME = "file"; // 싱크 커넥터의 옵션값으로 토픽은 추가할 필요없다.
    private static final String DIR_FILE_NAME_DEFAULT_VALUE = "/tmp/kafka.txt";
    private static final String DIR_FILE_NAME_DOC = "저장할 디렉토리와 파일 이름";

    public static final ConfigDef CONFIG = new ConfigDef().define(
            // 옵션값의 이름, 설명, 기본값, 중요도
            DIR_FILE_NAME, Type.STRING, DIR_FILE_NAME_DEFAULT_VALUE, ConfigDef.Importance.HIGH, DIR_FILE_NAME_DOC
    );

    public SingleFileSinkConnectorConfig(Map<String, String> props) {
        super(CONFIG, props);
    }
}
