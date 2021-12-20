package com.atom.spark.java;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * sparkSql 操作json
 *
 * @author Atom
 */
public class SparkSqlJdbc {
    public static void main(String[] args) {
        SparkSession session = SparkSession.builder().appName("use jdbc").config("spark.master", "local").getOrCreate();

        String url = "jdbc:mysql://localhost:3306/datago_integration_task";
        String name = "root";
        String password = "root";
        String table = "datago_directory";
        Dataset<Row> df = session.read().format("jdbc").option("url", url).option("dbtable", table).option("user", name).option("password", password).option("driver", "com.mysql.cj.jdbc.Driver").load();
        df.show();


        // 投影查询，只查 full_name 一个字段
        Dataset<Row> df2 = df.select(new Column("full_name"));
        df2.show();

        // 过滤
        df2 = df2.where("full_name like '/a%'");
        df2.show();

        // 写入, spark SQL 会自动创建表
        Properties prop = new Properties();
        prop.put("user", "root");
        prop.put("password", "root");
        prop.put("driver", "com.mysql.cj.jdbc.Driver");
        df2.write().jdbc(url, "atom_test_table", prop);
        df2.show();


    }
}
