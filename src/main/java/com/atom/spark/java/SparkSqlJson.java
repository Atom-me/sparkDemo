package com.atom.spark.java;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * sparkSql 操作json
 *
 * @author Atom
 */
public class SparkSqlJson {
    public static void main(String[] args) {
        SparkSession session = SparkSession.builder()
                .appName("read json")
                .config("spark.master", "local")
                .getOrCreate();

        Dataset<Row> df = session.read().json("file:///Users/atom/testDir/json.data");
//        df.createOrReplaceTempView("customers");
//        df = session.sql("select  * from customers");
        df.show(20);

    }
}
