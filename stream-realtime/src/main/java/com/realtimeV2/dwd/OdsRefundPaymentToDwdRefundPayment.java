package com.realtimeV2.dwd;

import com.realtimeV2.KafkaTopicUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/** ODS -> DWD 退款（refund_payment） */
public class OdsRefundPaymentToDwdRefundPayment {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.enableCheckpointing(5000);

        final String brokers = System.getProperty("kafka.brokers", "cdh01:9092");
        final String groupId = "dwd_refund_payment_group";

        KafkaTopicUtils.createTopic(brokers, "V2_dwd_refund_payment", 3, (short)2);

        tEnv.executeSql(
                "CREATE TABLE ods_professional ( " +
                        "  op STRING, " +
                        "  `after` ROW< " +
                        "     id BIGINT, order_id BIGINT, sku_id BIGINT, " +
                        "     refund_status STRING, subject STRING, " +
                        "     total_amount STRING, trade_no STRING, payment_type STRING, " +
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
                "CREATE TABLE dwd_refund_payment ( " +
                        "  id BIGINT, order_id BIGINT, sku_id BIGINT, " +
                        "  refund_status STRING, subject STRING, " +
                        "  total_amount STRING, trade_no STRING, payment_type STRING, " +
                        "  create_time BIGINT, operate_time BIGINT, callback_time BIGINT, " +
                        "  callback_content STRING, ts_ms BIGINT " +
                        ") WITH ( " +
                        "  'connector'='kafka', " +
                        "  'topic'='V2_dwd_refund_payment', " +
                        "  'properties.bootstrap.servers'='" + brokers + "', " +
                        "  'format'='json' " +
                        ")"
        );

        tEnv.executeSql(
                "INSERT INTO dwd_refund_payment " +
                        "SELECT " +
                        "  `after`.id, `after`.order_id, `after`.sku_id, " +
                        "  `after`.refund_status, `after`.subject, " +
                        "  `after`.total_amount, `after`.trade_no, `after`.payment_type, " +
                        "  `after`.create_time, `after`.operate_time, `after`.callback_time, " +
                        "  `after`.callback_content, ts_ms " +
                        "FROM ods_professional " +
                        "WHERE op IN ('c','u','r') " +
                        "  AND source.`table`='refund_payment' " +
                        "  AND `after`.id IS NOT NULL " +
                        "  AND `after`.order_id IS NOT NULL " +
                        "  AND CAST(`after`.total_amount AS DECIMAL(16,2)) >= 0"
        );

        System.out.println("退款 ODS->DWD 已启动");
    }
}
