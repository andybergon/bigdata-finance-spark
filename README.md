# finance-spark-from-kafka

Using spark streaming to calculate analytics on stock prices from kafka data.

## Run Spark Streaming
```
spark-submit \
 --class "it.himyd.spark.analysis.streaming.SparkStreamingAnalytics" \
 --master yarn \ 
 # --master local[4]
 finance-spark-from-kafka-0.0.1-SNAPSHOT.jar
```
 
 per usare jar esterni:
 ```
 --jars $HOME/x.jar
		$HOME/y.jar
```

## Cassandra
### Start Cassandra Service
`cassandra`
### Cassandra Query Language
`cqlsh`
