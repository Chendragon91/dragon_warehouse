package com.retailersv1;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.SQLUtil;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class DwdCancelOrderSQL {
    private static String TOPIC_DB = ConfigUtils.getString("kafka.cdc.db.topic");
    private static String DWD_CANCEL_ORDER = ConfigUtils.getString("kafka.dwd.cancel.order");
    private static String DWD_ORDER_TOPIC = ConfigUtils.getString("kafka.dwd.order.info");

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        tenv.getConfig().setIdleStateRetention(Duration.ofSeconds(30 * 60 + 5));
        env.setStateBackend(new MemoryStateBackend());

        tenv.executeSql("create table ods_professional(\n" +
                "    `op` string,\n" +
                "    `before` map<string,string>,\n" +
                "    `after` map<string,string>,\n" +
                "    `source` map<string,string>,\n" +
                "    `ts_ms` bigint,\n" +
                "    proc_time as proctime()\n" +
                ")" + SQLUtil.getKafka(TOPIC_DB, "test"));
//        tenv.executeSql("select * from ods_professional where `source`['table'] = 'order_info'" +
//                "and `op`='u'").print();

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
                        "ts bigint"+
                        ")" + SQLUtil.getKafka(DWD_ORDER_TOPIC, "test"));

//        tenv.executeSql("select * from dwd_order_info").print();


        Table orderCancel = tenv.sqlQuery("select " +
                " `after`['id'] id, " +
                " `after`['operate_time'] operate_time, " +
                " `ts_ms` as ts " +  // 设置别名ts
                "from ods_professional " +
                "where `source`['table']='order_info' " +
                "and `op`='u' " +
                "and `before`['order_status']='1001' " +
                "and `after`['order_status']='1003' ");
        tenv.createTemporaryView("order_cancel", orderCancel);

//        orderCancel.execute().print();

        Table result = tenv.sqlQuery(
                "select  " +
                        "od.id," +
                        "od.order_id," +
                        "od.user_id," +
                        "od.sku_id," +
                        "od.sku_name," +
                        "od.province_id," +
                        "od.activity_id," +
                        "od.activity_rule_id," +
                        "od.coupon_id," +
                        "date_format(oc.operate_time, 'yyyy-MM-dd') order_cancel_date_id," +
                        "oc.operate_time as cancel_time," +
                        "od.sku_num," +
                        "od.split_original_amount," +
                        "od.split_activity_amount," +
                        "od.split_coupon_amount," +
                        "od.split_total_amount," +
                        "oc.ts " +
                        "from dwd_order_info od " +
                        "join order_cancel oc " +
                        "on od.order_id=oc.id ");

//        result.execute().print();

        tenv.executeSql(
                "create table " + DWD_CANCEL_ORDER + "(" +
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
                        "cancel_time string," +
                        "sku_num string," +
                        "split_original_amount string," +
                        "split_activity_amount string," +
                        "split_coupon_amount string," +
                        "split_total_amount string," +
                        "ts bigint, " +
                        "PRIMARY KEY (id) NOT ENFORCED" +
                        ")" + SQLUtil.getUpsertKafkaDDL(DWD_CANCEL_ORDER));

        result.executeInsert(DWD_CANCEL_ORDER);
    }
}