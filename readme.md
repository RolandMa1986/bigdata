# 漏斗分析概述及Spark实现

漏斗分析是电商领域最常用的分析方法之一，主要用于分析一个多步用户流程中每一步的转化与流失。
                    
## 漏斗分析中的必要元素

### 窗口期
用户完成漏斗的时间限制，也即只有在这个时间范围内，用户从第一个步骤，行进到最后一个步骤，才能被视为一次成功的转化。
如果用户未能完成转换，也可用于计算中间步骤的转换率。

### 漏斗步骤
一个漏斗是由多个步骤组成的，这些步骤通常是用户完成某一业务流程最短的路径。以用户下单为例通常步骤分为：
* 浏览商品
* 将商品添加进购物车
* 结算购物车中的商品
* 选择送货地址、支付方式
* 点击付款
* 完成付款

对于每个步骤来说，最核心的指标就是转化率，公式如下：

** 转化率 = 通过该层的流量/到达该层的流量 **

### 筛选条件
除查看总体的转换率外，对于一款成熟产品用户还可以添加用户或事件的相关属性进行过滤，如用户性别，年龄等。

## 漏斗分析中的难点
在分析用户行为时，用户通常不会按照顺序完成漏斗中的所有步骤。假设漏斗步骤定义为A,B,C三步，窗口期为1天。那么只要用户在窗口期内完成A>B>C 三步，则为一次成功转换。在计算转换过程中通常使用以下规则:

1. 同一用户多次转换只计算一次
2. 忽略冗余的步骤，即A>X>B>X> C视为成功转换
3. 多次转换选择最靠近的转换步骤，即第一步到最后一步的最短路径。A > B > **A** > **B** > **C** 
4. 未成功转换时，选择最靠近下一次转换目标的转换如:
- **A** > **B** > A > B
- A > **A** > **B** > B

## Spark 基本步骤

### 1. 加载Json源数据
首先准备Json数据，并使用SparkSeason加载Json数据。注意:由于json中日期为字符串类型，因此需要显示转换为TimestampType类型。

``` java
        Dataset<Row> eventsDF = spark.read().json("src/main/resources/events.json");
        //Case datetime
        eventsDF = eventsDF.withColumn("eventTime", 
            date_format(new Column("eventTime"), "yyyy-MM-dd HH:mm:ss").cast(TimestampType));
        eventsDF.show();

```
查看执行结果:
| id   |   name   | userId |           eventTime |
| :--- | :------: | :----: | ------------------: |
| 1    | register |   1    | 2020-01-01 00:00:00 |
| 2    | register |   2    | 2020-01-01 00:00:00 |
| 3    | register |   3    | 2020-01-02 00:00:00 |
| 4    | register |   4    | 2020-01-02 00:00:00 |
| 5    | register |   5    | 2020-01-02 00:00:00 |
| 6    | register |   6    | 2020-01-03 00:00:00 |
| 7    | register |   7    | 2020-01-04 00:00:00 |
| 8    |   view   |   1    | 2020-01-01 00:00:00 |
| 9    |   view   |   2    | 2020-01-02 00:00:00 |
| 10   |   view   |   3    | 2020-01-02 00:00:00 |
| 11   |   view   |   4    | 2020-01-03 00:00:00 |
| 12   |   pay    |   1    | 2020-01-01 00:00:00 |
| 13   |   pay    |   2    | 2020-01-03 00:00:00 |
| 14   |   pay    |   3    | 2020-01-02 00:00:00 |
| 15   |   play   |   3    | 2020-01-02 00:00:00 |
| 16   |   view   |   2    | 2020-01-02 00:00:00 |

### 2. 过滤数据

``` java
        //将行数据转换为Event bean,并过滤数据，事件名为必要过滤条件，也可增加其他过滤条件，如：日期范围，用户属性等。
        Dataset<Event> eventsBeanDF = eventsDF.as(Encoders.bean(Event.class))
                .filter(new Column("name").isin("register", "view", "pay"));
        eventsBeanDF.cache();
        eventsBeanDF.show();
```

### 3. 数据清洗
由于项目中实现清洗转换步骤比较复杂，因此下面代码只使用去重来运算来演示用户的转换步骤。
首先使用groupByKey聚合函数将event按照用户进行分组操作。
然后对一个用户的所有操作进行去重后再展开。
``` java
   Dataset<Event> filteredEvents = eventsBeanDF
                .groupByKey((MapFunction<Event, Long>) Event::getUserId, Encoders.LONG())
                .flatMapGroups((FlatMapGroupsFunction<Long, Event, Event>) (userId, iterator) -> {
                    List<Event> funnelSteps = IteratorUtils.toList(iterator);
                    //TODO： only need return one event chain each user. This is only a fake code.
                    Iterator<Event> result =  funnelSteps.stream()
                            .sorted(Comparator.comparing(Event::getEventTime))
                            .filter(distinctByKey(b -> b.getName()))
                            .iterator();
                    return result;
                }, Encoders.bean(Event.class)); 
        //创建View，以便于后面步骤中使用SQL查询
        filteredEvents.createOrReplaceTempView("EventView");
        filteredEvents.show();
```
查看执行结果:
|id|    name|userId|          eventTime|
|:---| :------: | :----: | ------------------: |
| 7|register|     7|2020-01-04 00:00:00|
| 6|register|     6|2020-01-03 00:00:00|
| 5|register|     5|2020-01-02 00:00:00|
| 1|register|     1|2020-01-01 00:00:00|
| 8|    view|     1|2020-01-01 00:00:00|
|12|     pay|     1|2020-01-01 00:00:00|
| 3|register|     3|2020-01-02 00:00:00|
|10|    view|     3|2020-01-02 00:00:00|
|14|     pay|     3|2020-01-02 00:00:00|
| 2|register|     2|2020-01-01 00:00:00|
| 9|    view|     2|2020-01-02 00:00:00|
|13|     pay|     2|2020-01-03 00:00:00|
| 4|register|     4|2020-01-02 00:00:00|
|11|    view|     4|2020-01-03 00:00:00|



### 4. 使用CUBE聚合函数得到每一步的转换人数

最后通过CUBE函数对事件名和时间(天)进行聚合运算。

``` java
        Dataset<Row> cubedResult = spark.sql("SELECT name, string(date_trunc('day', eventTime)), count(*) AS count " +
                "FROM EventView GROUP BY CUBE (name, string(date_trunc('day', eventTime))) order by 1,2");
        cubedResult.show();
```

查看执行结果:
这里需要注意的是按照天天进行聚合的结果对漏斗操作是没有意义的，结果可以忽略。
|    name|CAST(date_trunc(day, eventTime) AS STRING)|count|
|    :---- |   :-----------------------------:  |   -----:|
| ~~null~~| ~~null~~|   14|
|~~null~~| ~~2020-01-01 00:00:00~~|    4|
| ~~null~~|          ~~2020-01-02 00:00:00~~|    6|
| ~~null~~|          ~~2020-01-03 00:00:00~~|    3|
| ~~null~~|          ~~2020-01-04 00:00:00~~|    1|
|     pay|                                      null|    3|
|     pay|                       2020-01-01 00:00:00|    1|
|     pay|                       2020-01-02 00:00:00|    1|
|     pay|                       2020-01-03 00:00:00|    1|
|register|                                      null|    7|
|register|                       2020-01-01 00:00:00|    2|
|register|                       2020-01-02 00:00:00|    3|
|register|                       2020-01-03 00:00:00|    1|
|register|                       2020-01-04 00:00:00|    1|
|    view|                                      null|    4|
|    view|                       2020-01-01 00:00:00|    1|
|    view|                       2020-01-02 00:00:00|    2|
|    view|                       2020-01-03 00:00:00|    1|

上表中总的转换率即为:

|事件名|转换人数|转换率|
|    :---- |  :-----:  |   -----:|
|register|   7|    |
|    view|  4|    4/7|
|     pay|     3|    3/4|
|总转换率|----|3/7|


## 参考

http://www.woshipm.com/data-analysis/758063.html

https://manual.sensorsdata.cn/sa/latest/guide_analytics_funnel-7540780.html







