package it.himyd.spark.analysis.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import it.himyd.cassandra.CassandraManager;
import it.himyd.kafka.KafkaConnector;
import it.himyd.spark.ml.clustering.StockClustererStreaming;
import it.himyd.stock.StockCluster;
import it.himyd.stock.StockOHLC;
import it.himyd.stock.finance.yahoo.Stock;

public class SparkStreamingAnalytics {
	private final static Duration BATCH_DURATION = Durations.seconds(1);
	private final static Duration WINDOW_DURATION = Durations.seconds(60);
	private final static Duration SLIDE_DURATION = Durations.seconds(5);

	public static void main(String args[]) throws Exception {
		String brokerAddress;
		String cassandraAddress;

		if (args.length != 2) {
			System.out.println(
					"Usage: java -cp SparkStreamingAnalytics-0.0.1-SNAPSHOT-jar-with-dependencies.jar <kafka-address> <cassandra-address");
			System.out.println("Setting localhost parameters...");

			brokerAddress = "localhost:9092";
			cassandraAddress = "localhost";
			// System.exit(1);
		} else {
			brokerAddress = args[0] + ":9092";
			cassandraAddress = args[1];
		}

		SparkConf conf = new SparkConf().setAppName("SparkStreamingAnalytics");
		conf.setMaster("local[2]"); // conf.setMaster("yarn");
		conf.set("spark.cassandra.connection.host", cassandraAddress);

		JavaStreamingContext jssc = new JavaStreamingContext(conf, BATCH_DURATION);

		KafkaConnector kc = new KafkaConnector(jssc, brokerAddress);

		JavaPairInputDStream<String, String> messages = kc.getStream();

		System.out.println("Starting analysis...");
		AnalysisRunner ar = new AnalysisRunner(WINDOW_DURATION, SLIDE_DURATION);
		JavaDStream<Stock> stocks = ar.convertKafkaMessagesToStock(messages);

		JavaDStream<StockOHLC> ohlc = ar.getOHLC(stocks);
		ohlc.print();
		
		kc.writeOHLC(ohlc);
		
		

		//StockClustererStreaming kms = new StockClustererStreaming();
		// JavaDStream<StockCluster> clusters = kms.clusterOHLC(ohlc);
		// ar.getSimilarStocks(clusters).print();

		// ar.printMostUp(stocks, 2);
		
		// CassandraManager cm = new CassandraManager();
		// cm.persistClusterStocks(clusters);

		jssc.start();
		jssc.awaitTermination();

	}

}
