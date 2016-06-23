package it.himyd.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import it.himyd.analysis.AnalysisRunner;
import it.himyd.finance.yahoo.Stock;
import it.himyd.kafka.KafkaConnector;
import it.himyd.spark.ml.clustering.StockClusterer;

public class SparkStockAnalytics {
	private final static Duration BATCH_DURATION = Durations.seconds(1);

	public final static int CLUSTERING_FEATURE_NUMBER = 3;

	public static void main(String s[]) throws Exception {

		// Create a local StreamingContext with two working thread and batch
		// interval of 1 second
		SparkConf conf = new SparkConf().setAppName("SparkStockAnalytics");
		conf.setMaster("local[2]");
		conf.set("spark.cassandra.connection.host", "localhost");

		JavaStreamingContext jssc = new JavaStreamingContext(conf, BATCH_DURATION);

		KafkaConnector kc = new KafkaConnector(jssc);
		JavaPairInputDStream<String, String> messages = kc.getStream();
		messages.print();

		AnalysisRunner ar = new AnalysisRunner();
		JavaDStream<Stock> stocks = ar.convertKafkaMessagesToStock(messages);
		// stocks.print();

		// JavaDStream<StockSample> sampleStocks =
		// ar.convertStockToStockSample(stocks);
		// sampleStocks.print();

		// StockClusterer kms = new StockClusterer();
		// kms.clusterStocks(stocks).print();

		JavaPairDStream<String, Double> average = ar.average(stocks);
		// average.print();

		JavaPairDStream<String, Double> percentage = ar.percentagePriceVariation(stocks);
		percentage.print();

		jssc.start();
		jssc.awaitTermination();

	}

}
