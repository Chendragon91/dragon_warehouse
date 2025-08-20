package com.retailersv1;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdPaymentInfoSQL {
    private static String TOPIC_DB = ConfigUtils.getString("kafka.cdc.db.topic");
    private static String DWD_PAYMENTINFO_TOPIC = ConfigUtils.getString("kafka.dwd.payment.info");

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 创建ods层表（CDC数据）
        tenv.executeSql("create table ods_professional(\n" +
                "    `op` string,\n" +
                "    `before` map<string,string>,\n" +
                "    `after` map<string,string>,\n" +
                "    `source` map<string,string>,\n" +
                "    `ts_ms` bigint,\n" +
                "    proc_time as proctime()\n" +  // 处理时间字段
                ")" + SQLUtil.getKafka(TOPIC_DB, "test"));


        // 创建订单详情表
        tenv.executeSql(
                "create table dwd_order_info(" +
                        "id string," +
                        "order_id string," +
                        "user_id string," +
                        "sku_id string," +
                        "sku_name string," +
                        "province_id string," +
                        "activity_id string," +
                        "activity_rule_id string," +
                        "coupon_id string," +
                        "date_id string," +
                        "create_time string," +
                        "sku_num string," +
                        "split_original_amount string," +
                        "split_activity_amount string," +
                        "split_coupon_amount string," +
                        "split_total_amount string," +
                        "ts bigint," +
                        "et as to_timestamp_ltz(ts, 0), " +
                        "watermark for et as et - interval '3' second " +
                        ")" + SQLUtil.getKafka(DWD_PAYMENTINFO_TOPIC, "test"));


        // 创建HBase基础字典表（终极解决方案：严格遵循HBase连接器规范）
        tenv.executeSql("create table base_dic(" +
                "rowkey string," +  // 唯一主键，仅保留rowkey作为HBase行键
                "dic_name string" +  // 仅保留必要字段，映射到HBase的info:dic_name
                ")" + SQLUtil.getHbaseDDL("dim_base_dic"));  // 依赖SQLUtil的HBase配置


        // 从CDC数据中过滤支付信息
        Table paymentInfo = tenv.sqlQuery("select " +
                "after['user_id'] user_id," +
                "after['order_id'] order_id," +
                "after['payment_type'] payment_type," +
                "after['callback_time'] callback_time," +
                "proc_time as pt," +
                "ts_ms as ts, " +
                "to_timestamp_ltz(ts_ms, 0) as et " +
                "from ods_professional " +
                "where `source`['table']='payment_info' " +
                "and `op`='u' " +
                "and `before`['payment_status'] is not null " +
                "and `after`['payment_status']='1602' ");
        tenv.createTemporaryView("payment_info", paymentInfo);


        // 三表关联（HBase关联条件必须是rowkey）
        Table result = tenv.sqlQuery(
                "select " +
                        "od.id order_detail_id," +
                        "od.order_id," +
                        "od.user_id," +
                        "od.sku_id," +
                        "od.sku_name," +
                        "od.province_id," +
                        "od.activity_id," +
                        "od.activity_rule_id," +
                        "od.coupon_id," +
                        "pi.payment_type payment_type_code ," +
                        "dic.dic_name payment_type_name," +  // 直接获取HBase中的名称字段
                        "pi.callback_time," +
                        "od.sku_num," +
                        "od.split_original_amount," +
                        "od.split_activity_amount," +
                        "od.split_coupon_amount," +
                        "od.split_total_amount split_payment_amount," +
                        "pi.ts " +
                        "from payment_info pi " +
                        "join dwd_order_info od " +
                        "on pi.order_id=od.order_id " +
                        "and od.et >= pi.et - interval '30' minute " +
                        "and od.et <= pi.et + interval '5' second " +
                        "join base_dic for system_time as of pi.pt as dic " +
                        "on pi.payment_type = dic.rowkey");  // 支付类型编码直接匹配rowkey


        // 写出到Kafka（添加主键约束）
        tenv.executeSql("create table " + DWD_PAYMENTINFO_TOPIC + "(" +
                "order_detail_id string," +
                "order_id string," +
                "user_id string," +
                "sku_id string," +
                "sku_name string," +
                "province_id string," +
                "activity_id string," +
                "activity_rule_id string," +
                "coupon_id string," +
                "payment_type_code string," +
                "payment_type_name string," +
                "callback_time string," +
                "sku_num string," +
                "split_original_amount string," +
                "split_activity_amount string," +
                "split_coupon_amount string," +
                "split_payment_amount string," +
                "ts bigint, " +
                "PRIMARY KEY (order_detail_id) NOT ENFORCED" +  // upsert-kafka必须主键
                ")" + SQLUtil.getUpsertKafkaDDL(DWD_PAYMENTINFO_TOPIC));

        result.executeInsert(DWD_PAYMENTINFO_TOPIC);
    }
}