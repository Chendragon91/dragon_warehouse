package com.realtimeV2.dwd;

import com.realtimeV2.KafkaTopicUtils;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ODS -> DWD 页面日志表
 */
public class OdsPageLogToDwdPageLog {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        final String BROKERS = System.getProperty("kafka.brokers", "cdh01:9092");
        final String GROUP_ID = System.getProperty("kafka.group.id", "ods_page_log_group");
        // 设置检查点（确保Exactly-Once语义）
        env.enableCheckpointing(5000);

        // 创建目标Topic
        KafkaTopicUtils.createTopic(BROKERS, "V2_dwd_page_log", 3, (short) 2);

        tEnv.executeSql(
                "CREATE TABLE ods_page_log (" +
                        " common ROW<" +
                        "   ar STRING, uid STRING, os STRING, ch STRING, is_new STRING, " +
                        "   md STRING, mid STRING, vc STRING, ba STRING, sid STRING" +
                        " >," +
                        " page ROW<" +
                        "   page_id STRING, during_time BIGINT, last_page_id STRING" +
                        " >," +
                        " ts BIGINT, " +
                        " row_time AS TO_TIMESTAMP_LTZ(ts, 3), " +
                        " WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND " +
                        ") WITH (" +
                        " 'connector'='kafka'," +
                        " 'topic'='page_log'," +
                        " 'properties.bootstrap.servers'='" + BROKERS + "'," +
                        " 'properties.group.id'='" + GROUP_ID + "'," +
                        " 'scan.startup.mode'='earliest-offset'," +
                        " 'format'='json'," +
                        " 'json.ignore-parse-errors'='true'" +
                        ")"
        );

        // 创建目标表
        tEnv.executeSql(
                "CREATE TABLE dwd_page_log (" +
                        " mid STRING, uid STRING, session_id STRING, province_id STRING, " +
                        " channel STRING, is_new STRING, page_id STRING, last_page_id STRING, " +
                        " item STRING, item_type STRING, during_time BIGINT, source_type STRING, ts BIGINT " +
                        ") WITH (" +
                        " 'connector'='kafka'," +
                        " 'topic'='V2_dwd_page_log'," +
                        " 'properties.bootstrap.servers'='" + BROKERS + "'," +
                        " 'format'='json'" +
                        ")"
        );

        System.out.println("开始执行数据清洗任务...");

        // 执行插入操作 - 使用TableEnvironment的execute方法
        tEnv.executeSql(
                "INSERT INTO dwd_page_log " +
                        "SELECT " +
                        " common.mid, common.uid, common.sid as session_id, common.ar as province_id, " +
                        " common.ch as channel, common.is_new, page.page_id, page.last_page_id, " +
                        " CAST(NULL AS STRING) as item, CAST(NULL AS STRING) as item_type, " +
                        " page.during_time, CAST(NULL AS STRING) as source_type, ts " +
                        "FROM ods_page_log " +
                        "WHERE common.mid IS NOT NULL AND page.page_id IS NOT NULL AND ts IS NOT NULL AND ts > 0"
        );

        System.out.println("数据清洗任务已启动，等待数据输入...");
        System.out.println("请向 page_log Topic发送测试数据");
    }
}