package com.realtimeV2.dim;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

import java.io.IOException;

/** 用户维度表 */
public class DimUserInfo {

    // HBase配置
    private static final String HBASE_ZOOKEEPER_QUORUM = "cdh01:2181";
    private static final String NAMESPACE = "realtime_V2";
    private static final String TABLE_NAME = "dim_user_info";
    private static final String[] COLUMN_FAMILIES = {"info"};

    public static void main(String[] args) throws Exception {
        // 首先创建HBase表
        createHBaseTable();

        // 然后执行Flink任务
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.enableCheckpointing(5000);

        final String brokers = "cdh01:9092";
        final String groupId = "dim_user_info_group";

        tEnv.executeSql(
                "CREATE TABLE ods_professional (" +
                        "  op STRING," +
                        "  `after` ROW<id BIGINT, login_name STRING, nick_name STRING, name STRING, " +
                        "              phone_num STRING, email STRING, user_level STRING, birthday BIGINT, gender STRING, create_time BIGINT>," +
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
                "CREATE TABLE dim_user_info (" +
                        "  rowkey STRING," +
                        "  info ROW<login_name STRING, nick_name STRING, name STRING, phone_num STRING, " +
                        "           email STRING, user_level STRING, birthday STRING, gender STRING, create_time STRING>," + // 修改为STRING类型
                        "  PRIMARY KEY (rowkey) NOT ENFORCED" +
                        ") WITH (" +
                        " 'connector'='hbase-2.2'," +
                        " 'table-name'='realtime_V2:dim_user_info'," +
                        " 'zookeeper.quorum'='cdh01:2181')" );

        tEnv.executeSql(
                "INSERT INTO dim_user_info " +
                        "SELECT CAST(`after`.id AS STRING), " +
                        "       ROW(`after`.login_name, `after`.nick_name, `after`.name, `after`.phone_num, " +
                        "           `after`.email, `after`.user_level, " +
                        "           CAST(`after`.birthday AS STRING), " + // 转换为字符串
                        "           `after`.gender, " +
                        "           CAST(`after`.create_time AS STRING)) " + // 转换为字符串
                        "FROM ods_professional " +
                        "WHERE op IN ('c','u','r') " +
                        "  AND source.`table`='user_info' " +
                        "  AND `after`.id IS NOT NULL").await();
    }

    /**
     * 创建HBase表和命名空间
     */
    private static void createHBaseTable() {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", HBASE_ZOOKEEPER_QUORUM);

        Connection connection = null;
        Admin admin = null;

        try {
            connection = ConnectionFactory.createConnection(config);
            admin = connection.getAdmin();

            // 检查并创建命名空间
            createNamespaceIfNotExists(admin, NAMESPACE);

            // 检查并创建表
            createTableIfNotExists(admin, NAMESPACE, TABLE_NAME, COLUMN_FAMILIES);

            System.out.println("HBase表创建成功: " + NAMESPACE + ":" + TABLE_NAME);

        } catch (IOException e) {
            System.err.println("创建HBase表时出错: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // 关闭资源
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 创建命名空间（如果不存在）
     */
    private static void createNamespaceIfNotExists(Admin admin, String namespace) throws IOException {
        try {
            admin.getNamespaceDescriptor(namespace);
            System.out.println("命名空间已存在: " + namespace);
        } catch (org.apache.hadoop.hbase.NamespaceNotFoundException e) {
            // 命名空间不存在，创建它
            NamespaceDescriptor namespaceDesc = NamespaceDescriptor.create(namespace).build();
            admin.createNamespace(namespaceDesc);
            System.out.println("命名空间创建成功: " + namespace);
        }
    }

    /**
     * 创建表（如果不存在）
     */
    private static void createTableIfNotExists(Admin admin, String namespace, String tableName, String[] columnFamilies) throws IOException {
        TableName fullTableName = TableName.valueOf(namespace + ":" + tableName);

        if (admin.tableExists(fullTableName)) {
            System.out.println("表已存在: " + fullTableName);
            return;
        }

        // 创建表描述符
        TableDescriptorBuilder tableBuilder = TableDescriptorBuilder.newBuilder(fullTableName);

        // 添加列族
        for (String cf : columnFamilies) {
            ColumnFamilyDescriptor cfDesc = ColumnFamilyDescriptorBuilder.newBuilder(cf.getBytes()).build();
            tableBuilder.setColumnFamily(cfDesc);
        }

        // 创建表
        admin.createTable(tableBuilder.build());
        System.out.println("表创建成功: " + fullTableName);
    }
}
