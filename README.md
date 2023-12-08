# Big-Data-Taobao-Behaviors
## Background and Framework
- Taobao is the largest online shopping website in China. Every day it generates billions of user data. I want to use my system to handle those data.
- My system include big data storage, migration, request and search.
- It uses the Lambda Architecture, which was divided into 3 layers. 
    - Batch layer is to store the immutable original data. Although in my case, there is only 4 GB data.
    - Speed layer is to ingest the streaming new data. 
    - Finally, serving layer will combine data from other two layers and generate views for users.

## How to run
- In the hadoop cluster, under `/home/hadoop/xqoasis/final/speed/target`:
```
[hadoop@ip-172-31-34-141 target]$ spark-submit --master local[2] --driver-java-options "-Dlog4j.configuration=file:///home/hadoop/ss.log4j.properties" --class StreamCategoryBehavior uber-speed-category-behavior-1.0-SNAPSHOT.jar b-3.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092,b-1.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092,b-2.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092
```
- In the EC2, under `/home/ec2-user/xqoasis/app2/app`:
```
[ec2-user@ip-172-31-16-171 app]$ node app.js 3058 ec2-3-131-137-149.us-east-2.compute.amazonaws.com 8070 b-3.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092,b-1.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092,b-2.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092
```
- Besides, in the hadoop cluster, you can monitor the kafka message queue.

## Data Source
- I downloaded the data from https://tianchi.aliyun.com/dataset/649. It needs an activated account. Therefore, I didn't use `wget` script to download that.
- Their are 2 data sources. The one is `UserBehavior.csv`, which includes:
![alt image](/pictures/data1.jpg)
- The behavior fields are -- pv: click times; fav: favorite; cart: added to shopping cart; buy: ordered
- The other is the `category.txt`. The code in it is the category code (the same as the category ID in `UserBehavior.csv`) and the name is the category decription.

## Batch Layer
- Firstly, I created ORD table `xqoasis_user_behavior` in Hive by reading csv file. Then I created the table `xqoasis_category` in Hive by reading txt file.
```
+---------------------------------------+-----------------------------------------+
| xqoasis_category_id_desc.category_id  | xqoasis_category_id_desc.category_desc  |
+---------------------------------------+-----------------------------------------+
| 1000858                               | Tools                                   |
| 1000959                               | Tools                                   |
| 1004392                               | Others                                  |
| 1007591                               | Others                                  |
| 1007732                               | Electronics                             |
| 1008109                               | Clothing & Shoes                        |
| 1008362                               | Clothing & Shoes                        |
| 1009182                               | Others                                  |
| 1010656                               | Clothing & Shoes                        |
| 1012333                               | Clothing & Shoes                        |
+---------------------------------------+-----------------------------------------+
```
- Secondly, I cleaned the data in `xqoasis_user_behavior`. I dropped the duplicated rows, and generate a new column called behavior_date, which is the date type value of column behavior_timestamp. The behavior_date's style is like '2017-11-25'.
- Thirdly, I dropped the empty and invalid value and make sure that all data is between '2017-11-25' and '2017-12-03' (this is what the data source told us).
- Finally, I joined the `xqoasis_user_behavior` and `xqoasis_category` to let us can see each category's description (i.e. name).
- All those tables are in the hive.
- This is the description of `xqoasis_category_behavior`.
```
+---------------------+------------+----------+
|      col_name       | data_type  | comment  |
+---------------------+------------+----------+
| user_id             | string     |          |
| item_id             | string     |          |
| category_id         | string     |          |
| category_desc       | string     |          |
| behavior_type       | string     |          |
| behavior_timestamp  | int        |          |
| behavior_date       | string     |          |
+---------------------+------------+----------+
```

```
+------------------------------------+------------------------------------+----------------------------------------+------------------------------------------+------------------------------------------+-----------------------------------------------+------------------------------------------+
| xqoasis_category_behavior.user_id  | xqoasis_category_behavior.item_id  | xqoasis_category_behavior.category_id  | xqoasis_category_behavior.category_desc  | xqoasis_category_behavior.behavior_type  | xqoasis_category_behavior.behavior_timestamp  | xqoasis_category_behavior.behavior_date  |
+------------------------------------+------------------------------------+----------------------------------------+------------------------------------------+------------------------------------------+-----------------------------------------------+------------------------------------------+
| 1                                  | 2087357                            | 2131531                                | Cell Phones                              | pv                                       | 1511975142                                    | 2017-11-29 17:05:42                      |
| 1                                  | 3911125                            | 982926                                 | Sports & Outdoors                        | pv                                       | 1512044958                                    | 2017-11-30 12:29:18                      |
+------------------------------------+------------------------------------+----------------------------------------+------------------------------------------+------------------------------------------+-----------------------------------------------+-------------------------------------
```


## Serving layer
- I wanted to let users can search for a specific category's each behavior (Page View, Added in Cart, Favorite, Buy)'s count.
- Therefore, I created a new table `xqoasis_category_behavior_sum` in Hbase. Each unique category is the key, and the column family is `behaviorSum`, which is the total count of each behavior (for this specific category).
```
hbase:016:0> scan 'xqoasis_category_behavior_sum',{'LIMIT' => 1}
ROW                                                                      COLUMN+CELL                                                                                                                                                                                                           
 Books                                                                   column=behaviorSum:buy, timestamp=2023-12-06T23:15:31.129, value=\x00\x00\x00\x00\x00\x03 \xFA                                                                                                                        
 Books                                                                   column=behaviorSum:cart, timestamp=2023-12-06T23:15:31.129, value=\x00\x00\x00\x00\x00\x08\xCE\xAA                                                                                                                    
 Books                                                                   column=behaviorSum:fav, timestamp=2023-12-06T23:15:31.129, value=\x00\x00\x00\x00\x00\x04`\x07                                                                                                                        
 Books                                                                   column=behaviorSum:pv, timestamp=2023-12-06T23:15:31.129, value=\x00\x00\x00\x00\x00\x91e1                                                                                                                            
1 row(s)
```
- In this case, I used the HBase with Hive script to do that.
- Also, I wanted to let users can search for a specific user_id's each behavior (Page View, Added in Cart, Favorite, Buy)'s count. (Although the external customer may not know the user id, I supposed this feature can be for the inner management.)
- In this case, I used the `Sparl.sql` to do that and created `xqoasis_user_behavior_count`.

### Dataframe and RDD practice
- I also want to practice my application of Spark OO stype programming (i.e. the application of RDD), but I didn't find a good way to apply this in my project. I used it to do some simple statistic.
- In my project, the RFM model is an important tool and means to measure customer value and customer profit-making ability. Three elements constitute the best indicators for data analysis, namely:
    - R-Recency (last purchase time)
    - F-Frequency (consumption frequency)
    - M-Money (consumption amount) (Those data was missing in my data source)
- Therefore, I use the spark RDD to do some analysis and generate a score for each user.

```
scala> xqoasisUserScoreMergeDF.show(5)
+-------+---+------+-------+---+------+-------+-----+
|user_id|  R|R_rank|R_score|  F|F_rank|F_score|score|
+-------+---+------+-------+---+------+-------+-----+
| 486458|  1|     1|      5|262|     1|      5|   10|
| 866670|  2|     2|      4|175|     2|      4|    8|
| 702034|  1|     1|      5|158|     3|      3|    8|
| 107013|  1|     1|      5|130|     4|      2|    7|
|1014116|  1|     1|      5|118|     5|      1|    6|
+-------+---+------+-------+---+------+-------+-----+
only showing top 5 rows
```
- After that, I merged the `RFM` table with `xqoasis_user_behavior_count` and get `xqoasis_user_behavior_count_score` to search operation.
```
ROW                                                                      COLUMN+CELL                                                                                                                                                                                                           
 100                                                                     column=info:F, timestamp=2023-12-08T02:53:48.953, value=8                                                                                                                                                             
 100                                                                     column=info:F_rank, timestamp=2023-12-08T02:53:48.953, value=81                                                                                                                                                       
 100                                                                     column=info:F_score, timestamp=2023-12-08T02:53:48.953, value=0                                                                                                                                                       
 100                                                                     column=info:R, timestamp=2023-12-08T02:53:48.953, value=6                                                                                                                                                             
 100                                                                     column=info:R_rank, timestamp=2023-12-08T02:53:48.953, value=6                                                                                                                                                        
 100                                                                     column=info:R_score, timestamp=2023-12-08T02:53:48.953, value=0                                                                                                                                                       
 100                                                                     column=info:buy, timestamp=2023-12-08T02:53:48.953, value=\x00\x00\x00\x00\x00\x00\x00\x08                                                                                                                            
 100                                                                     column=info:cart, timestamp=2023-12-08T02:53:48.953, value=\x00\x00\x00\x00\x00\x00\x00\x00                                                                                                                           
 100                                                                     column=info:fav, timestamp=2023-12-08T02:53:48.953, value=\x00\x00\x00\x00\x00\x00\x00\x03                                                                                                                            
 100                                                                     column=info:pv, timestamp=2023-12-08T02:53:48.953, value=\x00\x00\x00\x00\x00\x00\x00H                                                                                                                                
 100                                                                     column=info:score, timestamp=2023-12-08T02:53:48.953, value=0           
```


### Web Server
- In this layer, I implemented the webserver. I used the JavaScript with node.js, mustache to finish that. I deployed the webserver in the EC2.
- The `index.html` is the page for users to search by the category's name. It is on http://ec2-3-143-113-170.us-east-2.compute.amazonaws.com:3058/
- The app uses static resources in my application. Therefore, it is very convenient that users only type in url.
- The screenshot shows that user can search by category's name and know behavior information. The gray text is placeholder.
![alt image](/pictures/search-cate.jpg)


- The screenshot shows that user can search by user is and know information. The gray text is placeholder.
![alt image](/pictures/search-id.jpg)


## Speed Layer
- In the speed layer. I let the users type in user_id, item_id, category_id (suppose that user or managing staff don't know how we map the category, so they can only type in the original data), and each behavior's value in boolean type.
- After that, the information will be pushed to kafka massage queue in the topic `xqoasis_user_behavior`.
- For example:
```
[hadoop@ip-172-31-34-141 bin]$ ./kafka-console-consumer.sh --bootstrap-server b-1.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092 --topic xqoasis_user_behavior --from-beginning
{"user_id":"111","item_id":"111","category_id":"1000858","pv_val":true,"fav_val":false,"cart_val":false,"buy_val":false}
```
- To avoid user input dump information, I set the null value check in the html file. To help test, I set the placeholder. The placeholder of [category id] is the id of 'Tools'.
![alt image](/pictures/null-check.jpg)
- Finally, I deployed a scala program with `apache.spark.streaming` API. It will ingest the new data into our HBase table `xqoasis_category_behavior_sum`. It will firstly map the category id to category name.
- Then increase the value of behaviorSum for that specific category atomicly.
- The next time when users search for the category, will find the result was incremented if they typed in stream information before.
- The same way, I made an increment to the `xqoasis_user_behavior_count_score` table.
- The next time when users search for a specific user id, will find the result was incremented if they typed in stream information before.
- You can see attached video to see how it works.

https://drive.google.com/file/d/1h3HfyB8YNHqT4H42QMUGMu3MDAGfzXHo/view?usp=sharing

## Future work
- It will be more interesting if I can have some more dynamic graph. However, I have limited time and I am not very familiar with front-end development.
- Some BI tools might be helpful.
- Also, I noticed that most people are using flink intead of Spark.streaming, which is the next step I want to do.
