import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import java.util.HashMap;
import java.util.Map;

import static java.util.Arrays.asList;

public final class WriteData {

    public void main() throws InterruptedException {
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

        // More application logic would go here...
        // Create a RDD of 10 documents
        JavaRDD<Document> sparkDocuments = jsc.parallelize(asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).map
                (new Function<Integer, Document>() {
                    public Document call(final Integer i) throws Exception {
                        return Document.parse("{test: " + i + "}");
                    }
                });

        // Create a custom WriteConfig
        Map<String, String> writeOverrides = new HashMap<String, String>();
        writeOverrides.put("collection", "spark");
        writeOverrides.put("writeConcern.w", "majority");
        WriteConfig writeConfig = WriteConfig.create(jsc).withOptions(writeOverrides);


        /*Start Example: Save data from RDD to MongoDB*****************/
        MongoSpark.save(sparkDocuments, writeConfig);
        /*End Example**************************************************/


        jsc.close();

    }
}