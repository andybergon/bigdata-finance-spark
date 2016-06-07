# finance-spark-from-kafka

Using spark streaming to calculate analytics on stock prices from kafka data.

spark-submit
 --class "it.himyd.spark.SparkStockAnalytics"
 --master local[4]
 
 potrebbero servire:
 --packages "org.apache.spark:spark-streaming-twitter_2.10:1.5.1"
 --jars $HOME/spark-in-practice-1.0.jar
		$HOME/twitter4j-core-3.0.3.jar
