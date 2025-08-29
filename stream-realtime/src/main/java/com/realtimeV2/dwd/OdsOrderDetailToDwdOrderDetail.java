package com.realtimeV2.dwd;

import com.realtimeV2.KafkaTopicUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/** ODS -> DWD 订单明细（order_detail） */
public class OdsOrderDetailToDwdOrderDetail {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.enableCheckpointing(5000);

        final String brokers = System.getProperty("kafka.brokers", "cdh01:9092");
        final String groupId = "dwd_order_detail_group";

        KafkaTopicUtils.createTopic(brokers, "V2_dwd_order_detail", 3, (short)2);

        tEnv.executeSql(
                "CREATE TABLE ods_professional ( " +
                        "  op STRING, " +
                        "  `after` ROW< " +
                        "    id BIGINT, order_id BIGINT, sku_id BIGINT, sku_name STRING, " +
                        "    order_price STRING, sku_num BIGINT, " +
                        "    split_activity_amount STRING, split_coupon_amount STRING, split_total_amount STRING, " +
                        "    create_time BIGINT " +
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
                "CREATE TABLE dwd_order_detail ( " +
                        "  id BIGINT, order_id BIGINT, sku_id BIGINT, sku_name STRING, " +
                        "  order_price STRING, sku_num BIGINT, " +
                        "  split_activity_amount STRING, split_coupon_amount STRING, split_total_amount STRING, " +
                        "  create_time BIGINT, ts_ms BIGINT " +
                        ") WITH ( " +
                        "  'connector'='kafka', " +
                        "  'topic'='V2_dwd_order_detail', " +
                        "  'properties.bootstrap.servers'='" + brokers + "', " +
                        "  'format'='json' " +
                        ")"
        );

        tEnv.executeSql(
                "INSERT INTO dwd_order_detail " +
                        "SELECT " +
                        "  `after`.id, `after`.order_id, `after`.sku_id, `after`.sku_name, " +
                        "  `after`.order_price, `after`.sku_num, " +
                        "  `after`.split_activity_amount, `after`.split_coupon_amount, `after`.split_total_amount, " +
                        "  `after`.create_time, ts_ms " +
                        "FROM ods_professional " +
                        "WHERE op IN ('c','u','r') " +
                        "  AND source.`table`='order_detail' " +
                        "  AND `after`.id IS NOT NULL " +
                        "  AND `after`.order_id IS NOT NULL " +
                        "  AND `after`.sku_id IS NOT NULL " +
                        "  AND `after`.sku_num > 0 " +
                        "  AND CAST(`after`.order_price AS DECIMAL(16,2)) >= 0"
        );

        System.out.println("订单明细 ODS->DWD 已启动");
    }
}
