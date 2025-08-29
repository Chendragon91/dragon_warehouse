package com.realtimeV2.dim;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

import java.io.IOException;

/** SKU 商品维度表 */
public class DimSkuInfo {

    private static final String HBASE_ZOOKEEPER_QUORUM = "cdh01:2181";
    private static final String NAMESPACE = "realtime_V2";
    private static final String TABLE_NAME = "dim_sku_info";
    private static final String[] COLUMN_FAMILIES = {"info"};

    public static void main(String[] args) throws Exception {
        createHBaseTable();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.enableCheckpointing(5000);

        final String brokers = "cdh01:9092";
        final String groupId = "dim_sku_info_group";

        tEnv.executeSql(
                "CREATE TABLE ods_professional (" +
                        "  op STRING," +
                        "  `after` ROW<id BIGINT, spu_id STRING, price DECIMAL(16,2), sku_name STRING, category3_id BIGINT, create_time BIGINT>," +
                        "  source ROW<`table` STRING, db STRING>," +
                        "  ts_ms BIGINT" +
                        ") WITH (" +
                        " 'connector'='kafka'," +
                        " 'topic'='ods_professional'," +
                        " 'properties.bootstrap.servers'='" + brokers + "'," +
                        " 'properties.group.id'='" + groupId + "'," +
                        " 'scan.startup.mode'='earliest-offset'," +
                        " 'format'='json'," +
                        " 'json.ignore-parse-errors'='true')" );

        tEnv.executeSql(
                "CREATE TABLE dim_sku_info (" +
                        "  rowkey STRING," +
                        "  info ROW<spu_id STRING, price STRING, sku_name STRING, category3_id STRING, create_time STRING>," +
                        "  PRIMARY KEY (rowkey) NOT ENFORCED" +
                        ") WITH (" +
                        " 'connector'='hbase-2.2'," +
                        " 'table-name'='" + NAMESPACE + ":" + TABLE_NAME + "'," +
                        " 'zookeeper.quorum'='" + HBASE_ZOOKEEPER_QUORUM + "')" );

        tEnv.executeSql(
                "INSERT INTO dim_sku_info " +
                        "SELECT CAST(`after`.id AS STRING), " +
                        "       ROW(`after`.spu_id, CAST(`after`.price AS STRING), `after`.sku_name, CAST(`after`.category3_id AS STRING), CAST(`after`.create_time AS STRING)) " +
                        "FROM ods_professional " +
                        "WHERE op IN ('c','u','r') " +
                        "  AND source.`table`='sku_info' " +
                        "  AND `after`.id IS NOT NULL").await();
    }

    private static void createHBaseTable() {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", HBASE_ZOOKEEPER_QUORUM);

        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {

            createNamespaceIfNotExists(admin, NAMESPACE);
            createTableIfNotExists(admin, NAMESPACE, TABLE_NAME, COLUMN_FAMILIES);
            System.out.println("HBase表创建成功: " + NAMESPACE + ":" + TABLE_NAME);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void createNamespaceIfNotExists(Admin admin, String namespace) throws IOException {
        try {
            admin.getNamespaceDescriptor(namespace);
        } catch (org.apache.hadoop.hbase.NamespaceNotFoundException e) {
            NamespaceDescriptor namespaceDesc = NamespaceDescriptor.create(namespace).build();
            admin.createNamespace(namespaceDesc);
        }
    }

    private static void createTableIfNotExists(Admin admin, String namespace, String tableName, String[] columnFamilies) throws IOException {
        TableName fullTableName = TableName.valueOf(namespace + ":" + tableName);

        if (admin.tableExists(fullTableName)) {
            return;
        }

        TableDescriptorBuilder tableBuilder = TableDescriptorBuilder.newBuilder(fullTableName);
        for (String cf : columnFamilies) {
            ColumnFamilyDescriptor cfDesc = ColumnFamilyDescriptorBuilder.newBuilder(cf.getBytes()).build();
            tableBuilder.setColumnFamily(cfDesc);
        }
        admin.createTable(tableBuilder.build());
    }
}
