package com.realtimeV3.source;

import com.realtimeV3.model.CommentRecord;
import com.realtimeV3.util.ConfigLoader;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.io.IOException;
import java.util.Properties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;

/**
 * Kafka数据源连接器
 * 负责从Kafka topic消费评论数据
 */
public class KafkaCommentSource {

    private static final String DEFAULT_TOPIC = "jd_comments_topic";
    private static final String DEFAULT_GROUP = "jd_sentiment_group";

    public static KafkaSource<CommentRecord> createKafkaSource() {
        Properties kafkaProps = ConfigLoader.loadKafkaConfig();

        String bootstrapServers = kafkaProps.getProperty("bootstrap.servers", "cdh01:9092");
        String topic = kafkaProps.getProperty("topic.name", DEFAULT_TOPIC);
        String groupId = kafkaProps.getProperty("group.id", DEFAULT_GROUP);

        return KafkaSource.<CommentRecord>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new CommentRecordDeserializer())
                .setProperty("auto.offset.reset", "earliest")
                .setProperty("enable.auto.commit", "true")
                .setProperty("auto.commit.interval.ms", "1000")
                .setProperty("session.timeout.ms", "30000")
                .setProperty("max.poll.records", "500")
                .build();
    }

    /**
     * Kafka消息反序列化器
     * 将JSON字节数组转换为CommentRecord对象
     */
    private static class CommentRecordDeserializer implements DeserializationSchema<CommentRecord> {
        private static final long serialVersionUID = 1L;
        private static final ObjectMapper objectMapper;

        static {
            objectMapper = new ObjectMapper();
            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            objectMapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
        }

        @Override
        public CommentRecord deserialize(byte[] message) throws IOException {
            try {
                CommentRecord record = objectMapper.readValue(message, CommentRecord.class);

                // 基础验证
                if (record.getDataId() == null) {
                    record.setDataId(generateDataId());
                }
                if (record.getCommentTimestamp() == null) {
                    record.setCommentTimestamp(System.currentTimeMillis());
                }

                return record;

            } catch (Exception e) {
                System.err.println("JSON解析失败: " + e.getMessage());
                System.err.println("原始数据: " + new String(message).substring(0, Math.min(200, message.length)));
                return createErrorRecord(message, e);
            }
        }

        @Override
        public boolean isEndOfStream(CommentRecord nextElement) {
            return false;
        }

        @Override
        public TypeInformation<CommentRecord> getProducedType() {
            return TypeInformation.of(CommentRecord.class);
        }

        private CommentRecord createErrorRecord(byte[] message, Exception e) {
            long currentTime = System.currentTimeMillis();
            String errorData = new String(message);
            if (errorData.length() > 500) {
                errorData = errorData.substring(0, 500) + "...";
            }

            return new CommentRecord(
                    "err_" + currentTime + "_" + System.nanoTime(),
                    -999,
                    "error_product",
                    currentTime,
                    "数据解析错误",
                    "错误信息: " + e.getMessage() + " | 原始数据: " + errorData,
                    3,
                    currentTime
            );
        }

        private String generateDataId() {
            return "gen_" + System.currentTimeMillis() + "_" + (int)(Math.random() * 1000);
        }
    }
}