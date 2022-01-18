package com.atom.spark.java;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * 在使用jdbc开发时，一定要先启动thriftserver。
 * 在maven中添加hive-jdbc依赖
 *
 *
 * 配置 /usr/local/spark/conf/hive-site.xml
 * 在Spark-SQL的安装及使用的基础上，继续对hive-site.xml进行配置，如下
 *
 * <?xml version="1.0" encoding="UTF-8"?>
 *
 * <configuration>
 * <property>
 *   <name>hive.server2.thrift.port</name>
 *   <value>10001</value>
 * </property>
 * </configuration>
 * hive.metastore.uris指定了连接的Hive数据源，hive.server2.thrift.bind.host指定要启动thrift server的主机，
 * hive.server2.thrift.port指定要打开的端口号。
 * 使用端口10001是为了避免与Hive自己的hive.server2.thrift.port—10000产生冲突。
 *
 *
 * 实际上也可以通过命令!connect jdbc:hive2://hive-thrift-server:10000连接Hive自带的Thrift Server，
 * 与spark thrift server 10001 不同的是，连接HIVE自带的thrift server 1000 端口执行SQL， 每个sql语句都对应一个MR作业。
 *
 *
 * @author Atom
 */
public class SparkSqlHiveThriftServer {
    public static void main(String[] args) throws Exception {

        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection connection = DriverManager.getConnection("jdbc:hive2://10.16.118.247:10001");
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select * from mydb.tt");

        while (resultSet.next()) {
            int id = resultSet.getInt(1);
            String name = resultSet.getString(2);
            int age = resultSet.getInt(3);
            System.err.println(id + ":" + name + ":" + age);
        }
        resultSet.close();

    }
}
