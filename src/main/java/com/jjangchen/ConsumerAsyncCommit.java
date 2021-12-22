package com.jjangchen;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

// 비동기 오프셋 커밋
// 동기 오프셋 커밋보다 동일 시간당 데이터처리량이 많으나, 커밋이 실패할 경우 데이터의 중복 처리가 생길 수 있음.
public class ConsumerAsyncCommit {
    private final static Logger logger = LoggerFactory.getLogger(ConsumerWithSyncCommit.class);
    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVER = "localhost:9092";
    // subscribe메서드로 토픽을 구독할 시에는 필수 값
    private final static String GROUP_ID = "test_group";

    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(configs);
        consumer.subscribe(Arrays.asList(TOPIC_NAME));

        // 리턴된 가장 마지막 레코드를 기준으로 오프셋을 커밋
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

            for(ConsumerRecord<String, String> record : records) {
                logger.info("{}", record);
            }

            consumer.commitAsync();
        }
    }
}
