package it.himyd.spark.analysis.batch;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import it.himyd.cassandra.CassandraManager;
import it.himyd.stock.StockOHLC;
import it.himyd.stock.StockCluster;

public class SparkBatchAnalytics {

	String cassandraAddress;

	public static void main(String args[]) throws Exception {
		SparkBatchAnalytics sba = new SparkBatchAnalytics();
		sba.configure(args);
		sba.analyze();
	}

	private void configure(String[] args) {
		if (args.length != 1) {
			String usageString = "Usage: java -cp SparkBatchAnalytics-0.0.1-SNAPSHOT-jar-with-dependencies.jar <cassandra-address";
			System.out.println(usageString);

			System.out.println("Setting localhost parameters...");
			cassandraAddress = "localhost";

			// System.exit(1);
		} else {
			cassandraAddress = args[1];
		}
	}

	private void analyze() {
		SparkConf conf = new SparkConf().setAppName("SparkBatchAnalytics");
		conf.setMaster("local[2]");
		// conf.setMaster("yarn");
		conf.set("spark.cassandra.connection.host", this.cassandraAddress);

		JavaSparkContext jsc = new JavaSparkContext(conf);

		// Configuro il Cassandra Manager
		CassandraManager cm = new CassandraManager();
		cm.setClusterIP(this.cassandraAddress);
		cm.setResetData(false);
		cm.initialize();

		// Leggo OHLC da Cassandra
		JavaRDD<StockOHLC> ohlc = cm.readOHLCStocks(jsc);

		// Leggo Clusters da Cassandra
//		JavaRDD<StockCluster> clusters = cm.readClusterStocks(jsc);

		// Analizzo i dati Storici OHLC
		AnalysisBatchRunner abr = new AnalysisBatchRunner();
		abr.printPriceMostUp(ohlc, 5);
		// abr.printPriceMostDown(ohlc, 5);

		// Analizzo stock con movimenti comuni
		// abr.printSimilarStocks(clusters);

		jsc.stop();
	}

}
