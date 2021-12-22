package com.jjangchen;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

public class Consumer_RebalanceListener {
    private final static Logger logger = LoggerFactory.getLogger(ConsumerWithSyncCommit.class);
    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVER = "localhost:9092";
    // subscribe메서드로 토픽을 구독할 시에는 필수 값
    private final static String GROUP_ID = "test_group";
    private static KafkaConsumer<String, String> consumer;
    private static Map<TopicPartition, OffsetAndMetadata> currOffset = new HashMap<>();

    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        consumer = new KafkaConsumer<String, String>(configs);
        consumer.subscribe(Arrays.asList(TOPIC_NAME), new RebalanceListen());

        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for(ConsumerRecord record : records) {
                logger.info("{}", record);
                currOffset.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1, null));
                consumer.commitSync(currOffset);
            }
        }
    }

    private static class RebalanceListen implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            logger.warn("Partitions are revoked");
            consumer.commitSync(currOffset);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            logger.warn("Partitions are assigned");
        }
    }
}
