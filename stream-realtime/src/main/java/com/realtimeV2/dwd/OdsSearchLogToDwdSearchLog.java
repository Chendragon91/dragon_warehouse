package com.realtimeV2.dwd;

import com.realtimeV2.KafkaTopicUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ODS -> DWD 搜索日志
 * 源: search_log (Kafka, 嵌套: common/search/ts)
 * 目标: V2_dwd_search_log (Kafka)
 * 规则: keyword 非空且长度>0；ts>0
 */
public class OdsSearchLogToDwdSearchLog {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.enableCheckpointing(5000);

        final String BROKERS  = System.getProperty("kafka.brokers", "cdh01:9092");
        final String GROUP_ID = System.getProperty("kafka.group.id", "ods_search_log_group");

        KafkaTopicUtils.createTopic(BROKERS, "V2_dwd_search_log", 3, (short) 2);

        tEnv.executeSql(
                "CREATE TABLE ods_search_log (" +
                        "  common ROW<ar STRING, uid STRING, os STRING, ch STRING, is_new STRING, " +
                        "               md STRING, mid STRING, vc STRING, ba STRING, sid STRING>," +
                        "  `search` ROW<keyword STRING>," + // 使用反引号转义search关键字
                        "  ts BIGINT," +
                        "  row_time AS TO_TIMESTAMP_LTZ(ts, 3)," +
                        "  WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND" +
                        ") WITH (" +
                        "  'connector'='kafka'," +
                        "  'topic'='search_log'," +
                        "  'properties.bootstrap.servers'='" + BROKERS + "'," +
                        "  'properties.group.id'='" + GROUP_ID + "'," +
                        "  'scan.startup.mode'='earliest-offset'," +
                        "  'format'='json'," +
                        "  'json.ignore-parse-errors'='true')" );

        tEnv.executeSql(
                "CREATE TABLE dwd_search_log (" +
                        "  mid STRING, uid STRING, keyword STRING, ts BIGINT" +
                        ") WITH (" +
                        "  'connector'='kafka'," +
                        "  'topic'='V2_dwd_search_log'," +
                        "  'properties.bootstrap.servers'='" + BROKERS + "'," +
                        "  'format'='json')" );

        tEnv.executeSql(
                "INSERT INTO dwd_search_log " +
                        "SELECT common.mid, common.uid, `search`.keyword, ts " + // 使用反引号转义search关键字
                        "FROM ods_search_log " +
                        "WHERE `search`.keyword IS NOT NULL AND CHAR_LENGTH(TRIM(`search`.keyword)) > 0 " + // 使用CHAR_LENGTH替换LENGTH
                        "  AND ts IS NOT NULL AND ts > 0" );

        System.out.println("V2_dwd_search_log 作业已启动。");
    }
}