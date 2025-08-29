package com.realtimeV2.dwd;

import com.realtimeV2.KafkaTopicUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/** ODS -> DWD 专业属性（base_attr_value） */
public class OdsProfessionalToDwdProfessional {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.enableCheckpointing(5000);

        final String brokers = System.getProperty("kafka.brokers", "cdh01:9092");
        final String groupId = "dwd_professional_group";

        KafkaTopicUtils.createTopic(brokers, "V2_dwd_professional", 3, (short)2);

        tEnv.executeSql(
                "CREATE TABLE ods_professional ( " +
                        "  op STRING, " +
                        "  `after` ROW< " +
                        "     id BIGINT, attr_id BIGINT, value_name STRING, create_time BIGINT " +
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
                "CREATE TABLE dwd_professional ( " +
                        "  id BIGINT, attr_id BIGINT, value_name STRING, create_time BIGINT, ts_ms BIGINT " +
                        ") WITH ( " +
                        "  'connector'='kafka', " +
                        "  'topic'='V2_dwd_professional', " +
                        "  'properties.bootstrap.servers'='" + brokers + "', " +
                        "  'format'='json' " +
                        ")"
        );

        tEnv.executeSql(
                "INSERT INTO dwd_professional " +
                        "SELECT `after`.id, `after`.attr_id, `after`.value_name, `after`.create_time, ts_ms " +
                        "FROM ods_professional " +
                        "WHERE op IN ('c','u','r') " +
                        "  AND source.`table`='base_attr_value' " +
                        "  AND `after`.id IS NOT NULL " +
                        "  AND `after`.value_name IS NOT NULL"
        );

        System.out.println("专业属性 ODS->DWD 已启动");
    }
}
