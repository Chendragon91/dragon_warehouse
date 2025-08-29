package com.realtimeV2.dwd;

import com.realtimeV2.KafkaTopicUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/** ODS -> DWD 支付（payment_info） */
public class OdsPaymentInfoToDwdPaymentInfo {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.enableCheckpointing(5000);

        final String brokers = System.getProperty("kafka.brokers", "cdh01:9092");
        final String groupId = "dwd_payment_info_group";

        KafkaTopicUtils.createTopic(brokers, "V2_dwd_payment_info", 3, (short)2);

        tEnv.executeSql(
                "CREATE TABLE ods_professional ( " +
                        "  op STRING, " +
                        "  `after` ROW< " +
                        "     id BIGINT, order_id BIGINT, user_id BIGINT, " +
                        "     total_amount STRING, payment_status STRING, payment_type STRING, " +
                        "     out_trade_no STRING, subject STRING, " +
                        "     create_time BIGINT, operate_time BIGINT, callback_time BIGINT, " +
                        "     callback_content STRING " +
                        "  >, " +
                        "  source ROW<`table` STRING, db STRING>, " +
                        "  ts_ms BIGINT " +
                        ") WITH ( " +
                        "  'connector'='kafka', " +
                        "  'topic'='ods_professional', " +
                        "  'properties.bootstrap.servers'='" + brokers + "', " +
                        "  'properties.group.id'='" + groupId + "', " +
                        "  'scan.startup.mode'='earliest-offset', " +
                        "  'format'='json', " +
                        "  'json.ignore-parse-errors'='true' " +
                        ")"
        );

        tEnv.executeSql(
                "CREATE TABLE dwd_payment_info ( " +
                        "  id BIGINT, order_id BIGINT, user_id BIGINT, " +
                        "  total_amount STRING, payment_status STRING, payment_type STRING, " +
                        "  out_trade_no STRING, subject STRING, " +
                        "  create_time BIGINT, operate_time BIGINT, callback_time BIGINT, " +
                        "  callback_content STRING, ts_ms BIGINT " +
                        ") WITH ( " +
                        "  'connector'='kafka', " +
                        "  'topic'='V2_dwd_payment_info', " +
                        "  'properties.bootstrap.servers'='" + brokers + "', " +
                        "  'format'='json' " +
                        ")"
        );

        tEnv.executeSql(
                "INSERT INTO dwd_payment_info " +
                        "SELECT " +
                        "  `after`.id, `after`.order_id, `after`.user_id, " +
                        "  `after`.total_amount, `after`.payment_status, `after`.payment_type, " +
                        "  `after`.out_trade_no, `after`.subject, " +
                        "  `after`.create_time, `after`.operate_time, `after`.callback_time, " +
                        "  `after`.callback_content, ts_ms " +
                        "FROM ods_professional " +
                        "WHERE op IN ('c','u','r') " +
                        "  AND source.`table`='payment_info' " +
                        "  AND `after`.id IS NOT NULL " +
                        "  AND `after`.order_id IS NOT NULL " +
                        "  AND `after`.user_id IS NOT NULL " +
                        "  AND CAST(`after`.total_amount AS DECIMAL(16,2)) >= 0"
        );

        System.out.println("支付 ODS->DWD 已启动");
    }
}
