package com.realtimeV2.dws;

import com.realtimeV2.KafkaTopicUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwsTrafficOverview {

    private static final String BROKERS = "cdh01:9092";
    private static final String GROUP_ID = "dws_traffic_overview_group";

    public static void main(String[] args) throws Exception {

        // 1. Flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 创建 Kafka 目标 topic（分钟、小时、天）
        KafkaTopicUtils.createTopic(BROKERS, "dws_traffic_overview_minute", 3, (short) 2);
        KafkaTopicUtils.createTopic(BROKERS, "dws_traffic_overview_hour", 3, (short) 2);
        KafkaTopicUtils.createTopic(BROKERS, "dws_traffic_overview_day", 3, (short) 2);

        // 3. 读取 DWD 流量明细表
        tableEnv.executeSql(
                "CREATE TABLE dwd_page_log (" +
                        "   common MAP<STRING, STRING>, " +
                        "   page MAP<STRING, STRING>, " +
                        "   ts BIGINT, " +
                        "   et ARRAY<MAP<STRING, STRING>>, " +
                        "   user_id STRING, " +
                        "   session_id STRING, " +
                        "   rowtime AS TO_TIMESTAMP_LTZ(ts, 3), " +
                        "   WATERMARK FOR rowtime AS rowtime - INTERVAL '5' SECOND " +
                        ") WITH (" +
                        "   'connector' = 'kafka'," +
                        "   'topic' = 'V2_dwd_page_log'," +
                        "   'properties.bootstrap.servers' = '" + BROKERS + "'," +
                        "   'properties.group.id' = '" + GROUP_ID + "'," +
                        "   'format' = 'json'," +
                        "   'scan.startup.mode' = 'latest-offset'" +
                        ")"
        );

        // 4. 统计分钟级流量
        tableEnv.executeSql(
                "CREATE VIEW traffic_minute AS " +
                        "SELECT " +
                        "   DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss') AS stt, " +
                        "   DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss') AS edt, " +
                        "   DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm') AS cur_minute, " +
                        "   COUNT(*) AS pv, " +
                        "   COUNT(DISTINCT user_id) AS uv, " +
                        "   COUNT(DISTINCT session_id) AS sv, " +
                        "   SUM(CASE WHEN page['last_page_id'] IS NULL THEN 1 ELSE 0 END) AS uj " +
                        "FROM TABLE( " +
                        "   TUMBLE(TABLE dwd_page_log, DESCRIPTOR(rowtime), INTERVAL '1' MINUTE)" +
                        ") GROUP BY window_start, window_end"
        );

        // 5. 统计小时级流量
        tableEnv.executeSql(
                "CREATE VIEW traffic_hour AS " +
                        "SELECT " +
                        "   DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss') AS stt, " +
                        "   DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss') AS edt, " +
                        "   DATE_FORMAT(window_start, 'yyyy-MM-dd HH') AS cur_hour, " +
                        "   COUNT(*) AS pv, " +
                        "   COUNT(DISTINCT user_id) AS uv, " +
                        "   COUNT(DISTINCT session_id) AS sv, " +
                        "   SUM(CASE WHEN page['last_page_id'] IS NULL THEN 1 ELSE 0 END) AS uj " +
                        "FROM TABLE( " +
                        "   TUMBLE(TABLE dwd_page_log, DESCRIPTOR(rowtime), INTERVAL '1' HOUR)" +
                        ") GROUP BY window_start, window_end"
        );

        // 6. 统计天级流量
        tableEnv.executeSql(
                "CREATE VIEW traffic_day AS " +
                        "SELECT " +
                        "   DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss') AS stt, " +
                        "   DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss') AS edt, " +
                        "   DATE_FORMAT(window_start, 'yyyy-MM-dd') AS cur_date, " +
                        "   COUNT(*) AS pv, " +
                        "   COUNT(DISTINCT user_id) AS uv, " +
                        "   COUNT(DISTINCT session_id) AS sv, " +
                        "   SUM(CASE WHEN page['last_page_id'] IS NULL THEN 1 ELSE 0 END) AS uj " +
                        "FROM TABLE( " +
                        "   TUMBLE(TABLE dwd_page_log, DESCRIPTOR(rowtime), INTERVAL '1' DAY)" +
                        ") GROUP BY window_start, window_end"
        );

        // 7. Kafka Sink（分钟）
        tableEnv.executeSql(
                "CREATE TABLE dws_traffic_overview_minute (" +
                        "   stt STRING, " +
                        "   edt STRING, " +
                        "   cur_minute STRING, " +
                        "   pv BIGINT, " +
                        "   uv BIGINT, " +
                        "   sv BIGINT, " +
                        "   uj BIGINT " +
                        ") WITH (" +
                        "   'connector' = 'kafka'," +
                        "   'topic' = 'dws_traffic_overview_minute'," +
                        "   'properties.bootstrap.servers' = '" + BROKERS + "'," +
                        "   'format' = 'json'" +
                        ")"
        );

        // 8. Kafka Sink（小时）
        tableEnv.executeSql(
                "CREATE TABLE dws_traffic_overview_hour (" +
                        "   stt STRING, " +
                        "   edt STRING, " +
                        "   cur_hour STRING, " +
                        "   pv BIGINT, " +
                        "   uv BIGINT, " +
                        "   sv BIGINT, " +
                        "   uj BIGINT " +
                        ") WITH (" +
                        "   'connector' = 'kafka'," +
                        "   'topic' = 'dws_traffic_overview_hour'," +
                        "   'properties.bootstrap.servers' = '" + BROKERS + "'," +
                        "   'format' = 'json'" +
                        ")"
        );

        // 9. Kafka Sink（天）
        tableEnv.executeSql(
                "CREATE TABLE dws_traffic_overview_day (" +
                        "   stt STRING, " +
                        "   edt STRING, " +
                        "   cur_date STRING, " +
                        "   pv BIGINT, " +
                        "   uv BIGINT, " +
                        "   sv BIGINT, " +
                        "   uj BIGINT " +
                        ") WITH (" +
                        "   'connector' = 'kafka'," +
                        "   'topic' = 'dws_traffic_overview_day'," +
                        "   'properties.bootstrap.servers' = '" + BROKERS + "'," +
                        "   'format' = 'json'" +
                        ")"
        );

        // 10. 执行 INSERT 并阻塞
        TableResult res1 = tableEnv.executeSql("INSERT INTO dws_traffic_overview_minute SELECT * FROM traffic_minute");
        TableResult res2 = tableEnv.executeSql("INSERT INTO dws_traffic_overview_hour SELECT * FROM traffic_hour");
        TableResult res3 = tableEnv.executeSql("INSERT INTO dws_traffic_overview_day SELECT * FROM traffic_day");

        res1.getJobClient().get().getJobExecutionResult().get();
        res2.getJobClient().get().getJobExecutionResult().get();
        res3.getJobClient().get().getJobExecutionResult().get();
    }
}
