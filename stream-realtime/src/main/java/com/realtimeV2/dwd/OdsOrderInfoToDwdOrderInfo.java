package com.realtimeV2.dwd;

import com.realtimeV2.KafkaTopicUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/** ODS -> DWD 订单表（order_info） */
public class OdsOrderInfoToDwdOrderInfo {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.enableCheckpointing(5000);

        final String brokers = System.getProperty("kafka.brokers", "cdh01:9092");
        final String groupId = "dwd_order_info_group";

        KafkaTopicUtils.createTopic(brokers, "V2_dwd_order_info", 3, (short)2);

        // 源表：统一 ods_professional；字段与 txt 真实 JSON 完全对齐
        tEnv.executeSql(
                "CREATE TABLE ods_professional ( " +
                        "  op STRING, " +
                        "  `after` ROW< " +
                        "     id BIGINT, user_id BIGINT, province_id BIGINT, " +
                        "     consignee STRING, consignee_tel STRING, " +
                        "     order_status STRING, payment_way STRING, out_trade_no STRING, " +
                        "     trade_body STRING, original_total_amount STRING, total_amount STRING, " +
                        "     coupon_reduce_amount STRING, activity_reduce_amount STRING, " +
                        "     create_time BIGINT, operate_time BIGINT, refundable_time BIGINT " +
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
                "CREATE TABLE dwd_order_info ( " +
                        "  id BIGINT, user_id BIGINT, province_id BIGINT, " +
                        "  consignee STRING, consignee_tel STRING, " +
                        "  order_status STRING, payment_way STRING, out_trade_no STRING, " +
                        "  trade_body STRING, original_total_amount STRING, total_amount STRING, " +
                        "  coupon_reduce_amount STRING, activity_reduce_amount STRING, " +
                        "  create_time BIGINT, operate_time BIGINT, refundable_time BIGINT, " +
                        "  ts_ms BIGINT " +
                        ") WITH ( " +
                        "  'connector'='kafka', " +
                        "  'topic'='V2_dwd_order_info', " +
                        "  'properties.bootstrap.servers'='" + brokers + "', " +
                        "  'format'='json' " +
                        ")"
        );

        tEnv.executeSql(
                "INSERT INTO dwd_order_info " +
                        "SELECT " +
                        "  `after`.id, `after`.user_id, `after`.province_id, " +
                        "  `after`.consignee, `after`.consignee_tel, " +
                        "  `after`.order_status, `after`.payment_way, `after`.out_trade_no, " +
                        "  `after`.trade_body, `after`.original_total_amount, `after`.total_amount, " +
                        "  `after`.coupon_reduce_amount, `after`.activity_reduce_amount, " +
                        "  `after`.create_time, `after`.operate_time, `after`.refundable_time, " +
                        "  ts_ms " +
                        "FROM ods_professional " +
                        "WHERE op IN ('c','u','r') " +
                        "  AND source.`table`='order_info' " +
                        "  AND `after`.id IS NOT NULL " +
                        "  AND `after`.user_id IS NOT NULL " +
                        "  AND CAST(`after`.total_amount AS DECIMAL(16,2)) >= 0"
        );

        System.out.println("订单表 ODS->DWD 已启动");
    }
}
