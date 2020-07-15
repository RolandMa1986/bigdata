package org.roland.bigdata.spark;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.date_format;
import static org.apache.spark.sql.types.DataTypes.TimestampType;

public class FRMAnalysis {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("FunnelAnalysis")
                //.config("spark.some.config.option", "some-value")
                .getOrCreate();

        // 1. Load Data source and convert column
        Dataset<Row> tradesDF = spark.read().json("src/main/resources/trades.json");
        // 1.1 Case datetime
        tradesDF = tradesDF.withColumn("payTime", date_format(new Column("payTime"), "yyyy-MM-dd HH:mm:ss").cast(TimestampType));

        // Aggregate user's FRM
        tradesDF.createOrReplaceTempView("trade");
        Dataset<Row> frmDF = spark.sql("SELECT\n" +
                "      userId AS userId,\n" +
                "      datediff(current_date, DATE(max(payTime))) AS recency,\n" +
                "      count(*) AS frequency,\n" +
                "      sum(double(amount)) AS monetary,\n" +
                "      sum(double(amount)) / count(*) AS avgMonetary,\n" +
                "      min(payTime) AS firstOrderTime,\n" +
                "      max(payTime) AS lastOrderTime\n" +
                "    FROM trade\n" +
                "    GROUP BY userId");
        frmDF.createOrReplaceTempView("FRMView");

        //Group users
        Dataset<Row> frmGroupDF = spark.sql("SELECT\n" +
                "      f.userId,\n" +
                "      f.recency,\n" +
                "      f.frequency,\n" +
                "      f.monetary,\n" +
                "      f.avgMonetary,\n" +
                "      f.firstOrderTime,\n" +
                "      f.lastOrderTime,\n" +
                "      CASE\n" +
                "      WHEN recency <= 10 AND frequency >= 2 AND monetary >= 1000 THEN 'TypeA'\n" +
                "      WHEN recency > 10 AND frequency >= 2 AND monetary >= 1000 THEN 'TypeB'\n" +
                "      WHEN recency <= 10 AND frequency < 2 AND monetary >= 1000 THEN 'TypeC'\n" +
                "      WHEN recency > 10 AND frequency < 2 AND monetary >= 1000 THEN 'TypeD'\n" +
                "      WHEN recency > 10 AND frequency >= 2 AND monetary < 1000 THEN 'TypeE'\n" +
                "      WHEN recency <= 10 AND frequency < 2 AND monetary < 1000 THEN 'TypeF'\n" +
                "      WHEN recency <= 10 AND frequency >= 2 AND monetary < 1000 THEN 'TypeG'\n" +
                "      WHEN recency > 10 AND frequency < 2 AND monetary < 1000 THEN 'TypeH'\n" +
                "    END AS groupId\n" +
                "    FROM FRMView AS f");
        frmGroupDF.show();
    }
}
