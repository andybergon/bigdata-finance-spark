# finance-spark-from-kafka
Using Spark Streaming and Spark Batch to analyze stock prices.

## Run Spark Streaming
```
spark-submit \
 --class "it.himyd.spark.analysis.streaming.SparkStreamingAnalytics" \
 --master yarn \ 
 # --master local[4]
 finance-spark-from-kafka-0.0.1-SNAPSHOT.jar
```
 
to use external jars:
 ```
 --jars $HOME/x.jar
		$HOME/y.jar
```

## Cassandra
### Start Cassandra Service
`cassandra`
or
`cassandra -f`
### Cassandra Query Language
`cqlsh`
