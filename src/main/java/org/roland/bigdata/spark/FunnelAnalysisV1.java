package org.roland.bigdata.spark;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.function.FlatMapGroupsFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.roland.bigdata.model.Event;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.apache.spark.sql.functions.date_format;
import static org.apache.spark.sql.types.DataTypes.TimestampType;

/**
 * @author roland
 * @description This class shows how to create a simple funnel analysis.
 */
public class FunnelAnalysisV1 {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("FunnelAnalysis")
                //.config("spark.some.config.option", "some-value")
                .getOrCreate();

        // 1. Load Data source and convert column
        Dataset<Row> eventsDF = spark.read().json("src/main/resources/events.json");
        // 1.1 Case datetime
        eventsDF = eventsDF.withColumn("eventTime", date_format(new Column("eventTime"), "yyyy-MM-dd HH:mm:ss").cast(TimestampType));

        // 1.2 Filter Conditions:
        Dataset<Event> eventsBeanDF = eventsDF.as(Encoders.bean(Event.class))
                .filter(new Column("name").isin("register", "view", "pay"));

        // 1.2 Cache it
        eventsBeanDF.cache();
        eventsBeanDF.show();

        // 2. Map to Funnel Events
        Dataset<Event> filteredEvents = eventsBeanDF
                .groupByKey((MapFunction<Event, Long>) Event::getUserId, Encoders.LONG())
                .flatMapGroups((FlatMapGroupsFunction<Long, Event, Event>) (userId, iterator) -> {
                    List<Event> funnelSteps = IteratorUtils.toList(iterator);
                    //TODO： only need return one event chain each user. This is only a fake code.
                    AtomicReference<Event> first= new AtomicReference<>();
                    Iterator<Event> result =  funnelSteps.stream()
                            .sorted(Comparator.comparing(Event::getEventTime))
                            .map(event -> {
                                if(first.get() ==null){
                                    first.set(event);
                                } else {
                                    event.setEventTime(first.get().getEventTime());
                                }
                                return event;
                            })
                            .filter(distinctByKey(b -> b.getName()))
                            .iterator();
                    return result;
                }, Encoders.bean(Event.class));
        // 2.1 create a TempView
        filteredEvents.createOrReplaceTempView("EventView");
        filteredEvents.show();

        // 2.2 Use multiple dimension aggregations
        Dataset<Row> cubedResult = spark.sql("SELECT name, string(date_trunc('day', eventTime)), count(*) AS count " +
                "FROM EventView GROUP BY CUBE (name, string(date_trunc('day', eventTime))) order by 1,2");
        cubedResult.show();

    }


    private static <T> Predicate<T> distinctByKey(Function<? super T, ?> keyExtractor) {
        Map<Object, Boolean> seen = new ConcurrentHashMap<>();
        return t -> seen.putIfAbsent(keyExtractor.apply(t), Boolean.TRUE) == null;
    }
}
