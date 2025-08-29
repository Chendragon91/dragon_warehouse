package com.realtimeV2.dwd;

import com.realtimeV2.KafkaTopicUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ODS -> DWD 曝光日志
 * 源: display_log (Kafka, 嵌套: common/page/display, ts)
 * 目标: V2_dwd_exposure_log (Kafka)
 * 规则: 必须存在 item/page_id/pos_id；ts>0
 */
public class OdsDisplayLogToDwdExposureLog {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.enableCheckpointing(5000);

        final String BROKERS  = System.getProperty("kafka.brokers", "cdh01:9092");
        final String GROUP_ID = System.getProperty("kafka.group.id", "ods_display_log_group");

        KafkaTopicUtils.createTopic(BROKERS, "V2_dwd_exposure_log", 3, (short) 2);

        tEnv.executeSql(
                "CREATE TABLE ods_display_log (" +
                        "  common   ROW<ar STRING, uid STRING, os STRING, ch STRING, is_new STRING, " +
                        "                 md STRING, mid STRING, vc STRING, ba STRING, sid STRING>," +
                        "  page     ROW<page_id STRING, during_time BIGINT, last_page_id STRING>," +
                        "  display  ROW<item STRING, item_type STRING, pos_id STRING, pos_seq BIGINT>," + // 修改为单数display
                        "  ts BIGINT," +
                        "  row_time AS TO_TIMESTAMP_LTZ(ts, 3)," +
                        "  WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND" +
                        ") WITH (" +
                        "  'connector'='kafka'," +
                        "  'topic'='display_log'," +
                        "  'properties.bootstrap.servers'='" + BROKERS + "'," +
                        "  'properties.group.id'='" + GROUP_ID + "'," +
                        "  'scan.startup.mode'='earliest-offset'," +
                        "  'format'='json'," +
                        "  'json.ignore-parse-errors'='true')" );

        tEnv.executeSql(
                "CREATE TABLE dwd_exposure_log (" +
                        "  mid STRING, uid STRING, page_id STRING, item STRING, item_type STRING, pos_id STRING, ts BIGINT" +
                        ") WITH (" +
                        "  'connector'='kafka'," +
                        "  'topic'='V2_dwd_exposure_log'," +
                        "  'properties.bootstrap.servers'='" + BROKERS + "'," +
                        "  'format'='json')" );

        tEnv.executeSql(
                "INSERT INTO dwd_exposure_log " +
                        "SELECT o.common.mid, o.common.uid, o.page.page_id, " +
                        "       o.display.item, o.display.item_type, CAST(o.display.pos_id AS STRING), o.ts " + // 转换pos_id为STRING
                        "FROM ods_display_log AS o " +
                        "WHERE o.display.item IS NOT NULL " +
                        "  AND o.page.page_id IS NOT NULL " +
                        "  AND o.display.pos_id IS NOT NULL " +
                        "  AND o.ts IS NOT NULL AND o.ts > 0" );

        System.out.println("V2_dwd_exposure_log 作业已启动。");
    }
}