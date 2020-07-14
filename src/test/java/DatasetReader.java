import com.mongodb.spark.config.ReadConfig;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.junit.Test;
import org.roland.bigdata.model.User;
import org.roland.bigdata.service.MongoDBService;

import java.util.HashMap;
import java.util.Map;

public class DatasetReader {

    @Test
    public void main() throws InterruptedException {

        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .appName("MongoSparkConnectorIntro")
                .config("spark.mongodb.input.uri", "mongodb://bigdata:bigdata@127.0.0.1:27017/bigdata.customer")
                .config("spark.mongodb.output.uri", "mongodb://bigdata:bigdata@127.0.0.1:27017/bigdata.customer")
                .getOrCreate();

        // Create a JavaSparkContext using the SparkSession's SparkContext object
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());

        Map<String, String> readOverrides = new HashMap<String, String>();
        readOverrides.put("collection", "customer");
        readOverrides.put("readPreference.name", "secondaryPreferred");
        ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);

        MongoDBService datasetReader = new MongoDBService();

        Dataset<User> centenarians = datasetReader.load(sparkSession,readConfig,null, User.class);
        centenarians.show();

        Dataset<Row> rows = datasetReader.load(sparkSession,readConfig,null);
        rows.show();

        jsc.close();
    }



}
