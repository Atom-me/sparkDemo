package com.atom.spark.java;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * 在使用jdbc开发时，一定要先启动thriftserver。
 * 在maven中添加hive-jdbc依赖
 *
 * @author Atom
 */
public class SparkSqlHiveThriftServer {
    public static void main(String[] args) throws Exception {

        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection connection = DriverManager.getConnection("jdbc:hive2://10.16.118.247:10000");
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
