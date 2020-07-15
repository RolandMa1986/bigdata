# 基于RFM的客户价值分析及Spark实现
 RFM模型通过一个客户的近期购买行为、购买的总体频率以及花了多少钱3项指标来描述该客户的价值状况。

## RFM基本概念

### RFM的含义：

RFM分析就是根据客户活跃程度和交易金额的贡献，进行客户价值细分的一种方法。

- R（Recency）：客户最近一次交易时间的间隔。R值越大，表示客户交易发生的日期越久，反之则表示客户交易发生的日期越近。
- F（Frequency）：客户在最近一段时间内交易的次数。F值越大，表示客户交易越频繁，反之则表示客户交易不够活跃。
- M（Monetary）：客户在最近一段时间内交易的金额。M值越大，表示客户价值越高，反之则表示客户价值越低。

对于RFM评分可以采用区间段积分，例如将R分为5个区间，得分分别为1~5:

区间 | 得分
---|---
1<R<5 | 5
5<R<10 | 4
10<R<15 | 3
15<R<20 | 2
>=20 | 1


RFM的区间需要根据行业经验值进行设定。

### RFM用户分类
按照RFM三个维度得分与RS，FS，MS进行对比。我们可以将用户分为8类：

RFM用户类型	 | RS|FS|MS
---|---|---|---
高价值客户|大于|大于|大于
重点保持客户|小于|大于|大于
重点发展客户|大于|小于|大于
重点挽留客户|小于|小于|大于
一般价值客户|大于|大于|小于
一般保持客户|小于|大于|小于
一般发展客户|大于|小于|小于
潜在客户|小于|小于|小于

其中:
- RS阀值：交易间隔得分的平均值。
- FS阀值：交易频次得分的平均值。
- MS阀值：交易金额得分的平均值

## 简化版RFM分析的Spark实现
基于RFM区间评分与平均值阀值的实现中，依然依赖于实践经验设置有效的区间和评分，复杂的参数设置不能有效的反应出客户的RFM值。在实践过程中，通过设置RS、FS、MS阀值更为直观，对运营人员更为友好。

### 1. 加载交易数据
demo使用json格式的测试交易数据，并创建TempView
``` Java
Dataset<Row> tradesDF = spark.read().json("src/main/resources/trades.json");
tradesDF.createOrReplaceTempView("trade");
```

### 2. 查询过滤交易数据并计算RFM值
使用简单的集合函数求出 用户最后访问时间(recency)，访问次数(frequency)及消费总额(monetary)。作为其FRM值

``` SQL
SELECT
      userId AS userId,
      datediff(current_date, DATE(max(payTime))) AS recency,
      count(*) AS frequency,
      sum(double(amount)) AS monetary,
      sum(double(amount)) / count(*) AS avgMonetary,
      min(payTime) AS firstOrderTime,
      max(payTime) AS lastOrderTime
    FROM trade
    GROUP BY  userId
```
### 3. 基于FS、RS、MS阀值对用户分类
使用CASE语法进行分类
``` SQL
SELECT
      f.userId,
      f.recency,
      f.frequency,
      f.monetary,
      f.avgMonetary,
      f.firstOrderTime,
      f.lastOrderTime,
      CASE
      WHEN recency <= 10 AND frequency >= 2 AND monetary >= 1000 THEN 'TypeA'
      WHEN recency > 10 AND frequency >= 2 AND monetary >= 1000 THEN 'TypeB'
      WHEN recency <= 10 AND frequency < 2 AND monetary >= 1000 THEN 'TypeC'
      WHEN recency > 10 AND frequency < 2 AND monetary >= 1000 THEN 'TypeD'
      WHEN recency > 10 AND frequency >= 2 AND monetary < 1000 THEN 'TypeE'
      WHEN recency <= 10 AND frequency < 2 AND monetary < 1000 THEN 'TypeF'
      WHEN recency <= 10 AND frequency >= 2 AND monetary < 1000 THEN 'TypeG'
      WHEN recency > 10 AND frequency < 2 AND monetary < 1000 THEN 'TypeH'
    END AS groupId
    FROM FRMView AS f
```
执行结果:

|userId|recency|frequency|monetary|avgMonetary|     firstOrderTime|      lastOrderTime|groupId|
|------|-------|---------|--------|-----------|-------------------|-------------------|-------|
|     7|      0|        2|  2000.0|     1000.0|2020-01-01 00:00:00|2020-07-15 00:00:00|  TypeA|
|     6|    187|        2|  2000.0|     1000.0|2020-01-01 00:00:00|2020-01-10 00:00:00|  TypeB|
|     5|    196|        1|  1000.0|     1000.0|2020-01-01 00:00:00|2020-01-01 00:00:00|  TypeD|
|     1|    187|        2|   200.0|      100.0|2020-01-01 00:00:00|2020-01-10 00:00:00|  TypeE|
|     3|      0|        2|   200.0|      100.0|2020-01-01 00:00:00|2020-07-15 00:00:00|  TypeG|
|     8|      0|        1|  1000.0|     1000.0|2020-07-15 00:00:00|2020-07-15 00:00:00|  TypeC|
|     2|    196|        1|   100.0|      100.0|2020-01-01 00:00:00|2020-01-01 00:00:00|  TypeH|
|     4|      0|        1|   100.0|      100.0|2020-07-15 00:00:00|2020-07-15 00:00:00|  TypeF|


## 参考

https://help.aliyun.com/document_detail/135054.html

https://blog.csdn.net/weixin_44481878/article/details/90180452