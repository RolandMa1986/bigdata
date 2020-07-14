package org.roland.bigdata.spark;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singletonList;

public final class LoadData {

    public static void main(String[] args) throws InterruptedException {
        /* Create the SparkSession.
         * If config arguments are passed from the command line using --conf,
         * parse args for the values to set.
         */
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MongoSparkConnectorIntro")
                .config("spark.mongodb.input.uri", "mongodb://bigdata:bigdata@127.0.0.1:27017/bigdata.myCollection")
                .config("spark.mongodb.output.uri", "mongodb://bigdata:bigdata@127.0.0.1:27017/bigdata.myCollection")
                .getOrCreate();

        // Create a JavaSparkContext using the SparkSession's SparkContext object
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        // Create a custom ReadConfig
        Map<String, String> readOverrides = new HashMap<String, String>();
        readOverrides.put("collection", "spark");
        readOverrides.put("readPreference.name", "secondaryPreferred");
        ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);

        /*Start Example: Read data from MongoDB************************/
        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc, readConfig);


        /*End Example**************************************************/
        JavaMongoRDD<Document> aggregatedRdd = rdd.withPipeline(
                singletonList(
                        Document.parse("{ $match: { test : { $gt : 5 } } }")));

        // Analyze data from MongoDB
        System.out.println(rdd.count());
        System.out.println(aggregatedRdd.first().toJson());

        jsc.close();

    }
}