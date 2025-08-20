package com.retailersv1;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.SQLUtil;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class DwdDownOrderSQL {
    private static String TOPIC_DB = ConfigUtils.getString("kafka.cdc.db.topic");
    private static String DWD_ORDER_TOPIC = ConfigUtils.getString("kafka.dwd.order.info");

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter( env);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        tenv.getConfig().setIdleStateRetention(Duration.ofMinutes(5)); // 延长至5分钟
        // 在创建执行环境后添加
        env.setStateBackend(new MemoryStateBackend());

        tenv.executeSql("create table ods_professional(\n" +
                "    `op` string,\n" +
                "    `before` map<string,string>,\n" +
                "    `after` map<string,string>,\n" +
                "    `source` map<string,string>,\n" +
                "    `ts_ms` bigint,\n" +
                "    proc_time as proctime()\n" +
                ")"+ SQLUtil.getKafka(TOPIC_DB, "test"));

        Table orderDetail = tenv.sqlQuery("select\n" +
                "    `after`['id'] id,\n" +
                "    `after`['order_id'] order_id,\n" +
                "    `after`['sku_id'] sku_id,\n" +
                "    `after`['sku_name'] sku_name,\n" +
                "    `after`['create_time'] create_time,\n" +
                "    `after`['source_id'] source_id,\n" +
                "    `after`['source_type'] source_type,\n" +
                "    `after`['sku_num'] sku_num,\n" +
                "    cast(cast(`after`['sku_num'] as decimal(16,2))*\n" +
                "         cast(`after`['sku_price'] as decimal(16,2)) as string) split_original_amount,\n" +
                "    `after`['split_total_amount'] split_total_amount,\n" +
                "    `after`['split_activity_amount'] split_activity_amount,\n" +
                "    `after`['split_coupon_amount'] split_coupon_amount,\n" +
                "    ts_ms as ts\n" +
                "from ods_professional\n" +
                "where `source`['table']='order_detail'\n" +
                "and `op`='r'");

        tenv.createTemporaryView("order_detail", orderDetail);

//        orderDetail.execute().print();

        Table orderInfo = tenv.sqlQuery(
                "select " +
                        "after['id'] id," +
                        "after['user_id'] user_id," +
                        "after['province_id'] province_id " +
                        "from ods_professional " +
                        "where `source`['table']='order_info' " +
                        "and `op`='r' ");
        tenv.createTemporaryView("order_info", orderInfo);

//        orderInfo.execute().print();

        Table orderDetailActivity = tenv.sqlQuery(
                "select " +
                        "after['order_detail_id'] order_detail_id, " +
                        "after['activity_id'] activity_id, " +
                        "after['activity_rule_id'] activity_rule_id " +
                        "from ods_professional " +
                        "where `source`['table'] = 'order_detail_activity' " +
                        "and `op`='r' ");
        tenv.createTemporaryView("order_detail_activity", orderDetailActivity);

//        orderDetailActivity.execute().print();

        Table orderDetailCoupon = tenv.sqlQuery(
                "select " +
                        "after['order_detail_id'] order_detail_id, " +
                        "after['coupon_id'] coupon_id " +
                        "from ods_professional " +
                        "where `source`['table'] = 'order_detail_coupon' " +
                        "and `op`='r' ");
        tenv.createTemporaryView("order_detail_coupon", orderDetailCoupon);

//        orderDetailCoupon.execute().print();

        Table result = tenv.sqlQuery(
                "select " +
                        "od.id," +
                        "od.order_id," +
                        "oi.user_id," +
                        "od.sku_id," +
                        "od.sku_name," +
                        "oi.province_id," +
                        "act.activity_id," +
                        "act.activity_rule_id," +
                        "cou.coupon_id," +
                        "date_format(od.create_time, 'yyyy-MM-dd') date_id," +  // 年月日
                        "od.create_time," +
                        "od.sku_num," +
                        "od.split_original_amount," +
                        "od.split_activity_amount," +
                        "od.split_coupon_amount," +
                        "od.split_total_amount," +
                        "od.ts " +
                        "from order_detail od " +
                        "join order_info oi on od.order_id=oi.id " +
                        "left join order_detail_activity act " +
                        "on od.id=act.order_detail_id " +
                        "left join order_detail_coupon cou " +
                        "on od.id=cou.order_detail_id ");

//        result.execute().print();

        tenv.executeSql(
                "create table "+DWD_ORDER_TOPIC+"(" +
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
                        "primary key(id) not enforced " +
                        ")" + SQLUtil.getUpsertKafkaDDL(DWD_ORDER_TOPIC));

        result.executeInsert(DWD_ORDER_TOPIC);

    }
}
