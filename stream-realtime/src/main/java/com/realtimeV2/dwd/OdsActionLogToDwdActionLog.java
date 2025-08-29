package com.realtimeV2.dwd;

import com.realtimeV2.KafkaTopicUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ODS -> DWD 用户行为日志
 * 源: action_log (Kafka, 嵌套: common/page/action, ts)
 * 目标: V2_dwd_action_log (Kafka)
 * 规则: 仅保留 action_id 白名单 & 有 item 的记录；ts>0
 */
public class OdsActionLogToDwdActionLog {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.enableCheckpointing(5000);

        final String BROKERS  = System.getProperty("kafka.brokers", "cdh01:9092");
        final String GROUP_ID = System.getProperty("kafka.group.id", "ods_action_log_group");

        KafkaTopicUtils.createTopic(BROKERS, "V2_dwd_action_log", 3, (short) 2);

        tEnv.executeSql(
                "CREATE TABLE ods_action_log (" +
                        "  common  ROW<ar STRING, uid STRING, os STRING, ch STRING, is_new STRING, " +
                        "                md STRING, mid STRING, vc STRING, ba STRING, sid STRING>," +
                        "  page    ROW<page_id STRING, during_time BIGINT, last_page_id STRING>," +
                        "  action  ROW<action_id STRING, item STRING, item_type STRING, ts BIGINT>," + // 修改为单数action
                        "  row_time AS TO_TIMESTAMP_LTZ(action.ts, 3)," + // 使用action.ts作为时间戳
                        "  WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND" +
                        ") WITH (" +
                        "  'connector'='kafka'," +
                        "  'topic'='action_log'," +
                        "  'properties.bootstrap.servers'='" + BROKERS + "'," +
                        "  'properties.group.id'='" + GROUP_ID + "'," +
                        "  'scan.startup.mode'='earliest-offset'," +
                        "  'format'='json'," +
                        "  'json.ignore-parse-errors'='true')" );

        tEnv.executeSql(
                "CREATE TABLE dwd_action_log (" +
                        "  mid STRING, uid STRING, page_id STRING, action_id STRING, item STRING, item_type STRING, ts BIGINT" +
                        ") WITH (" +
                        "  'connector'='kafka'," +
                        "  'topic'='V2_dwd_action_log'," +
                        "  'properties.bootstrap.servers'='" + BROKERS + "'," +
                        "  'format'='json')" );

        tEnv.executeSql(
                "INSERT INTO dwd_action_log " +
                        "SELECT o.common.mid, o.common.uid, o.page.page_id, o.action.action_id, " +
                        "       o.action.item, o.action.item_type, o.action.ts " +
                        "FROM ods_action_log AS o " +
                        "WHERE o.action.action_id IN ('click','cart_add','favor','share','unfavor') " +
                        "  AND o.action.item IS NOT NULL " +
                        "  AND o.action.ts IS NOT NULL AND o.action.ts > 0" );

        System.out.println("V2_dwd_action_log 作业已启动。");
    }
}