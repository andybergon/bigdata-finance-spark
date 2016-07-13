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
	private final static Duration BATCH_DURATION = Durations.seconds(1); // ogni quanto prendo dati
	private final static Duration SLIDE_DURATION = Durations.seconds(10); // ogni quanto stampo analisi
	private final static Duration WINDOW_DURATION = Durations.seconds(60); // finestra dei dati analizzati

	String kafkaAddress;
	String cassandraAddress;

	boolean cassandraReset = false;
	int clusterNumber = 8;

	public static void main(String args[]) {
		SparkStreamingAnalytics ssa = new SparkStreamingAnalytics();
		ssa.configure(args);
		ssa.analyze();
	}

	private void configure(String[] args) {
		if (args.length != 2) {
			String usageString = "Usage: java -cp SparkStreamingAnalytics-0.0.1-SNAPSHOT-jar-with-dependencies.jar <kafka-address> <cassandra-address";
			System.out.println(usageString);

			System.out.println("Setting localhost parameters...");
			kafkaAddress = "localhost:9092";
			cassandraAddress = "localhost";

			// System.exit(1);
		} else {
			kafkaAddress = args[0] + ":9092";
			cassandraAddress = args[1];
		}
	}

	public void analyze() {
		SparkConf conf = new SparkConf().setAppName("SparkStreamingAnalytics");
		conf.setMaster("local[2]");
		// conf.setMaster("yarn");
		conf.set("spark.cassandra.connection.host", this.cassandraAddress);

		JavaStreamingContext jssc = new JavaStreamingContext(conf, BATCH_DURATION);

		KafkaConnector kc = new KafkaConnector(jssc, this.kafkaAddress);

		JavaPairInputDStream<String, String> messages = kc.getStream();

		System.out.println("Starting analysis...");
		AnalysisStreamingRunner ar = new AnalysisStreamingRunner(WINDOW_DURATION, SLIDE_DURATION);
		JavaDStream<Stock> stocks = ar.convertKafkaStringToStock(messages);

		// Calcolo l'aggregato OHLC su una finestra lunga WINDOW secondi ogni SLIDE secondi
		JavaDStream<StockOHLC> ohlc = ar.getOHLC(stocks);
		// ohlc.print();

		// Riscrive su Kafka gli aggregati OHLC
		// kc.writeOHLC(ohlc);

		// Calcolo quali K stock hanno andamenti % maggiori/minori nell'ultimo WINDOW time
		ar.printPriceMostUp(stocks, 5);
		// ar.printPriceMostDown(stocks, 5);
		// ar.printVolumeMostVariation(stocks, 5);

		// Calcolo K cluster sugli stock OHLC
		StockClustererStreaming kms = new StockClustererStreaming();
		kms.setClusterNumber(clusterNumber);
		JavaDStream<StockCluster> clusters = kms.clusterOHLC(ohlc);

		// Calcolo quali stock hanno più spesso lo stesso cluster
		// ar.printSameTrendStock(clusters, 5);

		// Configuro il Cassandra Manager
		CassandraManager cm = new CassandraManager();
		cm.setClusterIP(this.cassandraAddress);
		cm.setResetData(false);
		cm.initialize();

		// Persisto gli stock OHLC
		// cm.persistOHLCStocks(ohlc);

		// Persisto i cluster
		cm.persistClusterStocks(clusters);

		jssc.start();
		jssc.awaitTermination();
	}

}
