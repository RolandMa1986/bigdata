package org.roland.bigdata.spark;

import org.apache.spark.api.java.function.FlatMapGroupsFunction;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.roland.bigdata.model.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.spark.sql.functions.date_format;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.types.DataTypes.TimestampType;
/*
 * @description: Used for Test Only
 * @author Roland
 */
public class FunnelAnalysisTest {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("FunnelAnalysis")
//                .config("spark.some.config.option", "some-value")
                .getOrCreate();

//        Dataset<Row> userdf = spark.read().json("src/main/resources/users.json");
//        userdf.printSchema();
//        userdf.show();
//
//        Dataset<User> userschemaldf = userdf.as(Encoders.bean(User.class));
//        userschemaldf.printSchema();
//        userschemaldf.show();


        // 1. Load Data source and convert column
        Dataset<Row> eventdf = spark.read().json("src/main/resources/events.json");
        eventdf = eventdf.withColumn("eventTime", date_format(new Column("eventTime"), "yyyy-MM-dd HH:mm:ss").cast(TimestampType));
//        eventdf.printSchema();
        eventdf.show();
        // 1.1 Filter Conditions:
        Dataset<Event> eventschemaldf = eventdf.as(Encoders.bean(Event.class))
                .filter(new Column("name").isin("register", "view", "pay"));

        // 1.2 Cache it
        eventschemaldf.cache();
        eventschemaldf.show();

        Dataset<Event> filteredEvents = eventschemaldf
                .groupByKey((MapFunction<Event, Long>) Event::getUserId, Encoders.LONG())
                .flatMapGroups((FlatMapGroupsFunction<Long, Event, Event>) (userId, iterator) -> {
                    List<Event> funnelSteps = new ArrayList<>();
                    //TODO： 返回一个完整事件周期， this is only a fake code.
                    int i = -1;
                    while (iterator.hasNext()) {
                        Event next = iterator.next();
                        if (funnelSteps.stream().anyMatch(s -> s.getName().equals(next.getName()))) {
                            continue;
                        } else {
                            funnelSteps.add(next);
                            i++;
                        }
                    }
                    return funnelSteps.iterator();
                }, Encoders.bean(Event.class));
        filteredEvents.createOrReplaceTempView("EventView");
        filteredEvents.show();

        Dataset<Row> cubedResult = spark.sql("SELECT name, string(date_trunc('day', eventTime)), count(*) AS count " +
                "FROM EventView GROUP BY CUBE (name, string(date_trunc('day', eventTime))) order by 1,2");
        cubedResult.show();

//        String[] fieldNames = cubedResult.schema().fieldNames();
//        Dataset<Row> resultDS = cubedResult
//                .cube(Arrays.stream(fieldNames).filter(field -> !"count".equals(field)).map(colName -> functions.col(colName)).toArray(Column[]::new))
//                .agg(sum("count").as("count"))
//                .orderBy(Arrays.stream(fieldNames).map(colName -> functions.col(colName)).toArray(Column[]::new));

        //resultDS.show();
    }
}
