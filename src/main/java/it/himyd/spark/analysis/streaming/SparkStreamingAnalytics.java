package it.himyd.spark.analysis.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import it.himyd.kafka.KafkaConnector;
import it.himyd.persistence.cassandra.CassandraManager;
import it.himyd.spark.ml.clustering.StockClusterer;
import it.himyd.stock.StockCluster;
import it.himyd.stock.StockOHLC;
import it.himyd.stock.finance.yahoo.Stock;

public class SparkStreamingAnalytics {
	private final static Duration BATCH_DURATION = Durations.seconds(5);

	public final static int CLUSTERING_FEATURE_NUMBER = 3;

	public static void main(String s[]) throws Exception {

		SparkConf conf = new SparkConf().setAppName("SparkStockAnalytics");
		// working threads
		conf.setMaster("local[2]");
		conf.set("spark.cassandra.connection.host", "localhost");

		JavaStreamingContext jssc = new JavaStreamingContext(conf, BATCH_DURATION);

		KafkaConnector kc = new KafkaConnector(jssc);
		JavaPairInputDStream<String, String> messages = kc.getStream();
		// messages.print();

		AnalysisRunner ar = new AnalysisRunner();
		JavaDStream<Stock> stocks = ar.convertKafkaMessagesToStock(messages);
		// stocks.print();

		JavaDStream<StockOHLC> ohlc = ar.getOHLC(stocks);
		// ohlc.print();

		StockClusterer kms = new StockClusterer();
		JavaDStream<StockCluster> clusters = kms.clusterOHLC(ohlc);
		clusters.print();

		CassandraManager cm = new CassandraManager();
		cm.persistClusterStocks(clusters);

		jssc.start();
		jssc.awaitTermination();

	}

}
