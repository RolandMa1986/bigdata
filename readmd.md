## 转化筛选步骤

### 1. 源数据
| id   |  event   | userID |     date |
| :--- | :------: | :----: | -------: |
| 1    | register |   u1   | 2020-1-1 |
| 2    | register |   u2   | 2020-1-1 |
| 3    | register |   u3   | 2020-1-1 |
| 4    |   view   |   u1   | 2020-1-1 |
| 5    |   view   |   u2   | 2020-1-1 |
| 6    |   pay    |   u1   | 2020-1-1 |

#### 1.1 SQL filter condition: 
 select * from event where event in(''register,'view','pay') and date between **'datefrom'** and **'dateto'** 

Todo: add member join filter conditon

#### 1.2 Spark operation: 
1. need cache
2. create a tempView usereventlog

### 2. Map

| userID |            events |
| :----- | ----------------: |
| u1     | register>view>pay |
| u2     |     register>view |
| u3     |          register |

#### 2.1 Spark operation: 
1. 
groupByKey userID 
flatMapGroups events
2. create a tempView funnelsteps

### 3. Result

| steps | events |     date |
| :---- | :----: | -------: |
| step1 |   1   | 2020-1-1 |
| step2 |   2    | 2020-1-1 |
| step3 |  3    | 2020-1-1 |

#### 3.1 SQL filter condition: 
select steps, count(event),date from funnelsteps group by cube(steps,date)


