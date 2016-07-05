package it.himyd.spark.analysis.batch;

import java.util.Date;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import it.himyd.persistence.cassandra.CassandraManager;
import it.himyd.stock.StockCluster;
import scala.Tuple2;

public class SparkBatchAnalytics {

	public static void main(String s[]) throws Exception {

		SparkConf conf = new SparkConf().setAppName("SparkStockAnalytics");
		conf.setMaster("local[2]");
		conf.set("spark.cassandra.connection.host", "localhost");

		JavaSparkContext jsc = new JavaSparkContext(conf);

		CassandraManager cm = new CassandraManager();
		JavaRDD<StockCluster> clusters = cm.readClusterStocks(jsc);
		System.out.println(clusters.count());


		
		
		
		jsc.stop();
	}

}
