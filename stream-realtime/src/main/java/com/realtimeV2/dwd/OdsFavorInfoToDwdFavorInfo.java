package com.realtimeV2.dwd;

import com.realtimeV2.KafkaTopicUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/** ODS -> DWD 收藏（favor_info） */
public class OdsFavorInfoToDwdFavorInfo {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.enableCheckpointing(5000);

        final String brokers = System.getProperty("kafka.brokers", "cdh01:9092");
        final String groupId = "dwd_favor_info_group";

        KafkaTopicUtils.createTopic(brokers, "V2_dwd_favor_info", 3, (short)2);

        tEnv.executeSql(
                "CREATE TABLE ods_professional ( " +
                        "  op STRING, " +
                        "  `after` ROW< " +
                        "     id BIGINT, user_id BIGINT, sku_id BIGINT, spu_id BIGINT, " +
                        "     is_cancel STRING, create_time BIGINT, operate_time BIGINT " +
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
                "CREATE TABLE dwd_favor_info ( " +
                        "  id BIGINT, user_id BIGINT, sku_id BIGINT, spu_id BIGINT, " +
                        "  is_cancel STRING, create_time BIGINT, operate_time BIGINT, ts_ms BIGINT " +
                        ") WITH ( " +
                        "  'connector'='kafka', " +
                        "  'topic'='V2_dwd_favor_info', " +
                        "  'properties.bootstrap.servers'='" + brokers + "', " +
                        "  'format'='json' " +
                        ")"
        );

        tEnv.executeSql(
                "INSERT INTO dwd_favor_info " +
                        "SELECT " +
                        "  `after`.id, `after`.user_id, `after`.sku_id, `after`.spu_id, " +
                        "  `after`.is_cancel, `after`.create_time, `after`.operate_time, ts_ms " +
                        "FROM ods_professional " +
                        "WHERE op IN ('c','u','r') " +
                        "  AND source.`table`='favor_info' " +
                        "  AND `after`.id IS NOT NULL " +
                        "  AND `after`.user_id IS NOT NULL " +
                        "  AND `after`.sku_id IS NOT NULL"
        );

        System.out.println("收藏 ODS->DWD 已启动");
    }
}
