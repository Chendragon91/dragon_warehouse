package com.realtimeV2.dwd;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class dwd_traffic_page_view_inc {
    private static final String page_log = ConfigUtils.getString("kafka.page.topic");
    private static final String ods_cdc_db = ConfigUtils.getString("kafka.cdc.db.topic");

    public static void main(String[] args) throws Exception {
        // 1. 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 修复：为 ts 字段添加类型（假设是 BIGINT），并确保括号和字符串拼接正确
        tableEnv.executeSql("create table page_log(\n" +
                "    common map<string, string>,\n" +
                "    page map<string, string>,\n" +
                "    ts BIGINT\n" +  // 添加类型，例如 BIGINT
                ")" + SQLUtil.getKafka(page_log, "test"));  // 确保这里返回的是有效的 Kafka DDL 字符串

        tableEnv.executeSql("create table ods_professional(\n" +
                "    op map<string, string>,\n" +
                "    after map<string, string>,\n" +
                "    source map<string, string>,\n" +
                "    ts_ms BIGINT\n" +
                ")" + SQLUtil.getKafka(ods_cdc_db, "test"));

        Table OrderDetail = tableEnv.sqlQuery("select \n" +
                "    `after`['id'] as id,\n" +  // 添加别名
                "    `after`['order_id'] as order_id,\n" +
                "    `after`['sku_id'] as sku_id,\n" +
                "    `after`['sku_name'] as sku_name,\n" +
                "    `after`['order_price'] as order_price,\n" +
                "    `after`['sku_num'] as sku_num,\n" +
                "    `after`['create_time'] as create_time,\n" +
                "    `after`['split_total_amount'] as split_total_amount,\n" +
                "    `after`['split_activity_amount'] as split_activity_amount,\n" +
                "    `after`['split_coupon_amount'] as split_coupon_amount\n" +
                "from ods_professional\n" +
                "where `source`['table'] = 'order_detail'");

        tableEnv.createTemporaryView("order_detail", OrderDetail);



    }
}