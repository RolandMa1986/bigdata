package org.roland.bigdata.service;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import java.util.List;

public class MongoDBService {
    public <T> Dataset<T> load(
            SparkSession sparkSession, ReadConfig readConfig, List<Document> pipeline, Class <T> rawModelClass
    ) {
        JavaMongoRDD<Document> rdd = MongoSpark.load(
                new JavaSparkContext(sparkSession.sparkContext()),
                readConfig
        );
        if (CollectionUtils.isNotEmpty(pipeline)) {
            rdd = rdd.withPipeline(pipeline);
        }

        return rdd.toDS(rawModelClass);
    }

    public Dataset <Row> load(
            SparkSession sparkSession, ReadConfig readConfig, List <Document> pipeline
    ) {
        JavaMongoRDD<Document> rdd = MongoSpark.load(
                new JavaSparkContext(sparkSession.sparkContext()),
                readConfig
        );
        if (CollectionUtils.isNotEmpty(pipeline)) {
            rdd = rdd.withPipeline(pipeline);
        }

        return rdd.toDF();
    }
}
