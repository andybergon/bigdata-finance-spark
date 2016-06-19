# finance-spark-from-kafka

Using spark streaming to calculate analytics on stock prices from kafka data.

## Run Spark Streaming
```
spark-submit \
 --class "it.himyd.spark.SparkStockAnalytics" \
 --master local[4]
 
 
spark-submit --class "it.himyd.spark.SparkStockAnalytics" --master local[4] ~/git/bigdata-finance-spark/target/finance-spark-from-kafka-0.0.1-SNAPSHOT.jar
```
 
 potrebbero servire i parametri (con altri package ovviamente):
 ```
 --packages "org.apache.spark:spark-streaming-twitter_2.10:1.5.1"
 --jars $HOME/spark-in-practice-1.0.jar
		$HOME/twitter4j-core-3.0.3.jar
```
