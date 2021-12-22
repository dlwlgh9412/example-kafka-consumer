package com.jjangchen;

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

// 개별 레코드 단위로 매번 오프셋을 커밋
public class ConsumerWithSyncOffsetCommit {
    private final static Logger logger = LoggerFactory.getLogger(ConsumerWithSyncOffsetCommit.class);
    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVER = "localhost:9092";
    private final static String GROUP_ID = "test_group";

    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 명시적으로 오프셋 커밋을 수행하기 위한 설정
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(configs);
        consumer.subscribe(Arrays.asList(TOPIC_NAME));

        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

            // 오프셋을 커밋하기 위해 commitSync()의 파라미터
            // 키에는 파티션과 토픽이 들어가고, 값에는 오프셋정보가 들어간다.
            Map<TopicPartition, OffsetAndMetadata> currentOffset = new HashMap<>();
            for(ConsumerRecord<String, String> record : records) {
                logger.info("record: {}", record);
                // 오프셋은 +1을 해야하는데 이유는 컨슈머가 poll()을 수행할때 마지막으로 커밋한 오프셋부터 레코드를 리턴하기 때문
                currentOffset.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1, null));
                // 레코드 단위로 매번 오프셋을 커밋
                consumer.commitSync(currentOffset);
            }
        }
    }
}
