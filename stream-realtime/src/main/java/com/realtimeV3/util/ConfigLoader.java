package com.realtimeV3.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 配置加载工具类
 * 统一管理所有外部配置
 */
public class ConfigLoader {

    private static final Properties CACHE = new Properties();
    private static final long CACHE_TIMEOUT = 5 * 60 * 1000; // 5分钟缓存
    private static long lastLoadTime = 0;

    /**
     * 加载Kafka配置
     */
    public static Properties loadKafkaConfig() {
        return loadProperties("kafka.properties");
    }

    /**
     * 加载Doris配置
     */
    public static Properties loadDorisConfig() {
        return loadProperties("doris.properties");
    }

    /**
     * 加载Flink配置
     */
    public static Properties loadFlinkConfig() {
        return loadProperties("flink.properties");
    }

    /**
     * 从类路径加载Properties文件（带缓存）
     */
    private static synchronized Properties loadProperties(String fileName) {
        long currentTime = System.currentTimeMillis();

        // 检查缓存
        if (currentTime - lastLoadTime < CACHE_TIMEOUT && CACHE.containsKey(fileName)) {
            return (Properties) CACHE.get(fileName);
        }

        Properties props = new Properties();
        try (InputStream input = ConfigLoader.class.getClassLoader().getResourceAsStream(fileName)) {
            if (input == null) {
                System.out.println("配置文件 " + fileName + " 不存在，使用默认配置");
                props = getDefaultProperties(fileName);
            } else {
                props.load(input);
                System.out.println("成功加载配置文件: " + fileName);
            }

            // 更新缓存
            CACHE.put(fileName, props);
            lastLoadTime = currentTime;

        } catch (IOException e) {
            System.err.println("加载配置文件失败: " + fileName + ", 使用默认配置");
            props = getDefaultProperties(fileName);
            CACHE.put(fileName, props);
        }

        return props;
    }

    /**
     * 获取默认配置
     */
    private static Properties getDefaultProperties(String fileName) {
        Properties props = new Properties();

        switch (fileName) {
            case "kafka.properties":
                props.setProperty("bootstrap.servers", "cdh01:9092");
                props.setProperty("topic.name", "jd_comments_topic");
                props.setProperty("group.id", "jd_sentiment_group");
                props.setProperty("auto.offset.reset", "earliest");
                props.setProperty("enable.auto.commit", "true");
                props.setProperty("auto.commit.interval.ms", "1000");
                break;

            case "doris.properties":
                props.setProperty("fenodes", "127.0.0.1:8030");
                props.setProperty("database", "realtimeV3");
                props.setProperty("username", "root");
                props.setProperty("password", "123456");
                props.setProperty("connection.timeout", "30000");
                props.setProperty("socket.timeout", "60000");
                break;

            case "flink.properties":
                props.setProperty("checkpoint.interval", "5000");
                props.setProperty("parallelism.default", "2");
                props.setProperty("restart.strategy", "fixed-delay");
                props.setProperty("restart.attempts", "3");
                props.setProperty("restart.delay", "10000");
                props.setProperty("buffer.timeout", "100");
                props.setProperty("taskmanager.memory.process.size", "2048m");
                break;

            default:
                System.err.println("未知配置文件: " + fileName);
        }

        return props;
    }

    /**
     * 获取指定配置值
     */
    public static String getProperty(String fileName, String key) {
        return getProperty(fileName, key, null);
    }

    /**
     * 获取指定配置值（带默认值）
     */
    public static String getProperty(String fileName, String key, String defaultValue) {
        Properties props = loadProperties(fileName);
        return props.getProperty(key, defaultValue);
    }

    /**
     * 获取整数配置值
     */
    public static int getIntProperty(String fileName, String key, int defaultValue) {
        try {
            String value = getProperty(fileName, key);
            return value != null ? Integer.parseInt(value) : defaultValue;
        } catch (NumberFormatException e) {
            System.err.println("配置值不是整数: " + key);
            return defaultValue;
        }
    }

    /**
     * 获取布尔配置值
     */
    public static boolean getBooleanProperty(String fileName, String key, boolean defaultValue) {
        String value = getProperty(fileName, key);
        return value != null ? Boolean.parseBoolean(value) : defaultValue;
    }

    /**
     * 清除配置缓存
     */
    public static void clearCache() {
        CACHE.clear();
        lastLoadTime = 0;
    }

    /**
     * 重新加载所有配置
     */
    public static void reloadAll() {
        clearCache();
        loadKafkaConfig();
        loadDorisConfig();
        loadFlinkConfig();
        System.out.println("所有配置已重新加载");
    }
}