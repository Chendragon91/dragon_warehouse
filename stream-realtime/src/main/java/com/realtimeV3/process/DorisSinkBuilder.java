package com.realtimeV3.process;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.SimpleStringSerializer;
import org.apache.flink.connector.base.DeliveryGuarantee;
import com.realtimeV3.util.ConfigLoader;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Doris Sink构建器
 * 负责创建和配置Doris写入连接
 */
public class DorisSinkBuilder {

    private static final String DEFAULT_FENODES = "127.0.0.1:8030";
    private static final String DEFAULT_DATABASE = "realtimeV3";
    private static final String DEFAULT_USERNAME = "root";
    private static final String DEFAULT_PASSWORD = "123456";

    /**
     * 构建Doris Sink
     */
    public static DorisSink<String> buildDorisSink(String tableName) {
        Properties dorisProps = ConfigLoader.loadDorisConfig();

        String fenodes = dorisProps.getProperty("fenodes", DEFAULT_FENODES);
        String database = dorisProps.getProperty("database", DEFAULT_DATABASE);
        String username = dorisProps.getProperty("username", DEFAULT_USERNAME);
        String password = dorisProps.getProperty("password", DEFAULT_PASSWORD);

        DorisOptions dorisOptions = DorisOptions.builder()
                .setFenodes(fenodes)
                .setTableIdentifier(database + "." + tableName)
                .setUsername(username)
                .setPassword(password)
                .build();

        // StreamLoad配置
        Properties sinkProperties = new Properties();
        sinkProperties.setProperty("format", "json");
        sinkProperties.setProperty("read_json_by_line", "true");
        sinkProperties.setProperty("strip_outer_array", "true");
        sinkProperties.setProperty("columns",
                "data_id, user_id, product_id, comment_time, comment_title, " +
                        "comment_content, original_rating, sentiment_score, comment_length, " +
                        "has_positive_words, has_negative_words, sentiment_category, " +
                        "sentiment_confidence, process_time, product_category, " +
                        "hour_of_day, day_of_week, is_consistent");

        // 执行选项配置
        DorisExecutionOptions executionOptions = DorisExecutionOptions.builder()
                .setLabelPrefix("doris_sink_" + tableName + "_" + System.currentTimeMillis())
                .setDeletable(false)
                .setBufferCount(10000)
                .setBufferSize(8 * 1024 * 1024) // 8MB缓冲区
                .setBufferFlushMaxRows(10000)
                .setBufferFlushMaxBytes(8 * 1024 * 1024)
                .setBufferFlushIntervalMs(3000)
                .setMaxRetries(5)
                .setRetryBackoffMultiplierMs(1000)
                .setMaxRetryBackoffMs(10000)
                .setEnableDelete(false)
                .setCheckInterval(3000)
                .setStreamLoadProp(sinkProperties)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setIgnoreUpdateBefore(true)
                .build();

        return DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder()
                        .setDeserializeArrowAsync(false)
                        .setExecMemLimit(2147483648L) // 2GB
                        .setRequestQueryTimeoutS(3600)
                        .setRequestBatchSize(1000)
                        .setRequestConnectTimeoutMs(30000)
                        .setRequestReadTimeoutMs(60000)
                        .setRequestRetries(3)
                        .build())
                .setDorisOptions(dorisOptions)
                .setDorisExecutionOptions(executionOptions)
                .setSerializer(new SimpleStringSerializer())
                .build();
    }

    /**
     * 构建ODS层Sink
     */
    public static DorisSink<String> buildOdsSink() {
        return buildDorisSink("ods_jd_comments");
    }

    /**
     * 构建DWD层Sink
     */
    public static DorisSink<String> buildDwdSink() {
        return buildDorisSink("dwd_jd_comment_sentiment");
    }

    /**
     * 构建指定表的Sink（带自定义配置）
     */
    public static DorisSink<String> buildCustomSink(String tableName, Properties customProps) {
        Properties dorisProps = ConfigLoader.loadDorisConfig();

        // 合并自定义配置
        Properties mergedProps = new Properties();
        mergedProps.putAll(dorisProps);
        mergedProps.putAll(customProps);

        String fenodes = mergedProps.getProperty("fenodes", DEFAULT_FENODES);
        String database = mergedProps.getProperty("database", DEFAULT_DATABASE);
        String username = mergedProps.getProperty("username", DEFAULT_USERNAME);
        String password = mergedProps.getProperty("password", DEFAULT_PASSWORD);

        DorisOptions dorisOptions = DorisOptions.builder()
                .setFenodes(fenodes)
                .setTableIdentifier(database + "." + tableName)
                .setUsername(username)
                .setPassword(password)
                .build();

        Properties sinkProperties = new Properties();
        sinkProperties.setProperty("format", "json");
        sinkProperties.setProperty("read_json_by_line", "true");

        DorisExecutionOptions executionOptions = DorisExecutionOptions.builder()
                .setLabelPrefix("custom_sink_" + tableName)
                .setBufferCount(Integer.parseInt(mergedProps.getProperty("buffer.count", "10000")))
                .setBufferSize(Integer.parseInt(mergedProps.getProperty("buffer.size", "8388608")))
                .setBufferFlushIntervalMs(Integer.parseInt(mergedProps.getProperty("flush.interval", "3000")))
                .setMaxRetries(Integer.parseInt(mergedProps.getProperty("max.retries", "5")))
                .setStreamLoadProp(sinkProperties)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        return DorisSink.<String>builder()
                .setDorisOptions(dorisOptions)
                .setDorisExecutionOptions(executionOptions)
                .setSerializer(new SimpleStringSerializer())
                .build();
    }
}