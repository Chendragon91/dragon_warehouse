package com.realtimeV2.dwd;

import com.realtimeV2.KafkaTopicUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ODS -> DWD 启动日志表
 * 源: start_log (Kafka, 嵌套: common/start/ts)
 * 目标: V2_dwd_start_log (Kafka)
 * 规则: mid 必须存在；entry 属于白名单；ts>0
 */
public class OdsStartLogToDwdStartLog {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.enableCheckpointing(5000);

        final String BROKERS  = System.getProperty("kafka.brokers", "cdh01:9092");
        final String GROUP_ID = System.getProperty("kafka.group.id", "ods_start_log_group");

        KafkaTopicUtils.createTopic(BROKERS, "V2_dwd_start_log", 3, (short) 2);

        tEnv.executeSql(
                "CREATE TABLE ods_start_log (" +
                        "  common ROW<ar STRING, uid STRING, os STRING, ch STRING, is_new STRING, " +
                        "               md STRING, mid STRING, vc STRING, ba STRING, sid STRING>," +
                        "  `start`  ROW<entry STRING, loading_time BIGINT, open_ad_id STRING, open_ad_ms BIGINT>," +
                        "  ts BIGINT," +
                        "  row_time AS TO_TIMESTAMP_LTZ(ts, 3)," +
                        "  WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND" +
                        ") WITH (" +
                        "  'connector'='kafka'," +
                        "  'topic'='start_log'," +
                        "  'properties.bootstrap.servers'='" + BROKERS + "'," +
                        "  'properties.group.id'='" + GROUP_ID + "'," +
                        "  'scan.startup.mode'='earliest-offset'," +
                        "  'format'='json'," +
                        "  'json.ignore-parse-errors'='true')" );

        tEnv.executeSql(
                "CREATE TABLE dwd_start_log (" +
                        "  mid STRING, uid STRING, channel STRING, is_new STRING, " +
                        "  entry STRING, loading_time BIGINT, open_ad_id STRING, open_ad_ms BIGINT, ts BIGINT" +
                        ") WITH (" +
                        "  'connector'='kafka'," +
                        "  'topic'='V2_dwd_start_log'," +
                        "  'properties.bootstrap.servers'='" + BROKERS + "'," +
                        "  'format'='json')" );

        tEnv.executeSql(
                "INSERT INTO dwd_start_log " +
                        "SELECT common.mid, common.uid, common.ch AS channel, common.is_new, " +
                        "       `start`.entry, `start`.loading_time, `start`.open_ad_id, `start`.open_ad_ms, ts " +
                        "FROM ods_start_log " +
                        "WHERE common.mid IS NOT NULL " +
                        "  AND `start`.entry IN ('icon','notification','share') " +
                        "  AND ts IS NOT NULL AND ts > 0" );

        System.out.println("V2_dwd_start_log 作业已启动。");
    }
}