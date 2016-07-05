package it.himyd.spark.analysis.batch;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;

import it.himyd.persistence.cassandra.CassandraManager;
import it.himyd.stock.StockCluster;

public class SparkBatchAnalytics {

	public static void main(String s[]) throws Exception {

		SparkConf conf = new SparkConf().setAppName("SparkStockAnalytics");
		conf.setMaster("local[2]");
		conf.set("spark.cassandra.connection.host", "localhost");

		SparkContext sc = new SparkContext(conf);

		CassandraManager cm = new CassandraManager();
		JavaRDD<StockCluster> clusters = cm.readClusterStocks();

	}

}
