package com.retailersv1;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class HbaseMysqlCommentDicDwdSQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 对于Kafka流处理，通常需要设置检查点
        env.enableCheckpointing(5000);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        // 从Kafka读取comment_info数据
        TableResult kafkaTable = tenv.executeSql("" +
                "CREATE TABLE comment_info (" +
                "  id BIGINT," +
                "  user_id BIGINT," +
                "  nick_name VARCHAR(20)," +
                "  head_img VARCHAR(200)," +
                "  sku_id BIGINT," +
                "  spu_id BIGINT," +
                "  order_id BIGINT," +
                "  appraise VARCHAR(10)," +
                "  comment_txt VARCHAR(200)," +
                "  create_time TIMESTAMP(3)," +
                "  operate_time TIMESTAMP(3)," +
                "  -- Kafka连接器需要的元数据字段" +
                "  proc_time AS PROCTIME(), " +  // 处理时间属性
                "  event_time AS create_time,  " +  // 事件时间属性
                "  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND " +  // 水印设置
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = 'ods_professional'," +  // Kafka主题名
                "  'properties.bootstrap.servers' = 'cdh01:9092,cdh02:9092,cdh03:9092'," +  // Kafka brokers
                "  'scan.startup.mode' = 'earliest-offset'," +  // 启动模式：最早位点
                "  'format' = 'json'," +  // 数据格式为JSON
                "  'json.fail-on-missing-field' = 'false'," +  // 缺失字段不失败
                "  'json.ignore-parse-errors' = 'true'" +  // 忽略解析错误
                ")"
        );


        // HBase表定义保持不变
        TableResult hbaseTable = tenv.executeSql("" +
                "CREATE TABLE dim_base_dic (" +
                "  rowkey STRING," +
                "  info ROW<" +
                "    dic_code STRING," +
                "    dic_name STRING," +
                "    parent_code STRING," +
                "    create_time INT," +
                "    operate_time INT" +
                "  >," +
                "  PRIMARY KEY (rowkey) NOT ENFORCED" +
                ") WITH (" +
                "  'connector' = 'hbase-2.2'," +
                "  'table-name' = 'dim_base_dic'," +
                "  'zookeeper.quorum' = 'cdh01'," +
                "  'zookeeper.znode.parent' = '/hbase'" +
                ")"
        );


        // 关联查询逻辑保持不变
        Table joinedResult = tenv.sqlQuery(
                "SELECT " +
                        "  c.id AS comment_id, " +
                        "  c.user_id, " +
                        "  c.nick_name, " +
                        "  c.appraise AS appraise_code, " +
                        "  d.info.dic_name AS appraise_name, " +
                        "  c.sku_id, " +
                        "  c.comment_txt, " +
                        "  c.create_time AS comment_time, " +
                        "  TO_TIMESTAMP_LTZ(CAST(d.info.create_time AS BIGINT) * 1000, 3) AS dic_create_time, " +
                        "  TO_TIMESTAMP_LTZ(CAST(d.info.operate_time AS BIGINT) * 1000, 3) AS dic_operate_time " +
                        "FROM comment_info c " +
                        "LEFT JOIN dim_base_dic d " +
                        "  ON c.appraise = d.info.dic_code"
        );


        joinedResult.execute().print();
        env.execute("Kafka and HBase Join Job");
    }
}
