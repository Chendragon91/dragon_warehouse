package com.realtimeV2;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Kafka Topic自动创建工具类
 */
public class KafkaTopicUtils {

    // 使用字符串常量代替Kafka配置常量
    private static final String RETENTION_MS_CONFIG = "retention.ms";
    private static final String CLEANUP_POLICY_CONFIG = "cleanup.policy";
    private static final String CLEANUP_POLICY_DELETE = "delete";

    /**
     * 创建单个Topic
     */
    public static boolean createTopic(String bootstrapServers, String topicName,
                                      int partitions, short replicationFactor,
                                      Map<String, String> configs) {
        try (AdminClient adminClient = createAdminClient(bootstrapServers)) {
            NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);

            if (configs != null && !configs.isEmpty()) {
                newTopic.configs(configs);
            }

            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
            System.out.println("Topic创建成功: " + topicName);
            return true;
        } catch (ExecutionException e) {
            if (e.getCause().getClass().getSimpleName().equals("TopicExistsException")) {
                System.out.println("Topic已存在: " + topicName);
                return true;
            }
            System.err.println("创建Topic失败: " + topicName + ", 错误: " + e.getMessage());
            return false;
        } catch (Exception e) {
            System.err.println("创建Topic异常: " + topicName + ", 错误: " + e.getMessage());
            return false;
        }
    }

    /**
     * 创建单个Topic（使用默认配置）
     */
    public static boolean createTopic(String bootstrapServers, String topicName,
                                      int partitions, short replicationFactor) {
        Map<String, String> defaultConfigs = new HashMap<>();
        defaultConfigs.put(RETENTION_MS_CONFIG, "604800000"); // 7天 retention
        defaultConfigs.put(CLEANUP_POLICY_CONFIG, CLEANUP_POLICY_DELETE);

        return createTopic(bootstrapServers, topicName, partitions, replicationFactor, defaultConfigs);
    }

    /**
     * 批量创建多个Topic
     */
    public static boolean createTopics(String bootstrapServers, Map<String, TopicConfig> topics) {
        try (AdminClient adminClient = createAdminClient(bootstrapServers)) {
            for (Map.Entry<String, TopicConfig> entry : topics.entrySet()) {
                TopicConfig config = entry.getValue();
                NewTopic newTopic = new NewTopic(entry.getKey(),
                        config.getPartitions(), config.getReplicationFactor());

                if (config.getConfigs() != null) {
                    newTopic.configs(config.getConfigs());
                }

                adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
                System.out.println("Topic创建成功: " + entry.getKey());
            }
            return true;
        } catch (Exception e) {
            System.err.println("批量创建Topic异常: " + e.getMessage());
            return false;
        }
    }

    /**
     * 检查Topic是否存在
     */
    public static boolean topicExists(String bootstrapServers, String topicName) {
        try (AdminClient adminClient = createAdminClient(bootstrapServers)) {
            return adminClient.listTopics().names().get().contains(topicName);
        } catch (Exception e) {
            System.err.println("检查Topic存在性失败: " + e.getMessage());
            return false;
        }
    }

    /**
     * 创建AdminClient
     */
    private static AdminClient createAdminClient(String bootstrapServers) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
        return AdminClient.create(props);
    }

    /**
     * Topic配置类
     */
    public static class TopicConfig {
        private int partitions;
        private short replicationFactor;
        private Map<String, String> configs;

        public TopicConfig(int partitions, short replicationFactor) {
            this.partitions = partitions;
            this.replicationFactor = replicationFactor;
            this.configs = new HashMap<>();
        }

        public TopicConfig(int partitions, short replicationFactor, Map<String, String> configs) {
            this.partitions = partitions;
            this.replicationFactor = replicationFactor;
            this.configs = configs != null ? configs : new HashMap<>();
        }

        public int getPartitions() { return partitions; }
        public short getReplicationFactor() { return replicationFactor; }
        public Map<String, String> getConfigs() { return configs; }

        public TopicConfig withConfig(String key, String value) {
            this.configs.put(key, value);
            return this;
        }
    }
}