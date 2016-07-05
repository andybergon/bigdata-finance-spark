package it.himyd.spark.analysis.batch;

import java.util.Date;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
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

		SparkContext sc = new SparkContext(conf);

		CassandraManager cm = new CassandraManager();
		JavaRDD<StockCluster> clusters = cm.readClusterStocks(sc);
		System.out.println(clusters.count());

		JavaPairRDD<String, String> date2cluster = clusters.mapToPair(new PairFunction<StockCluster, String, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(StockCluster t) throws Exception {

				return new Tuple2<>(t.getClustertime() + "--" + t.getCluster(), t.getSymbol());
			}
		});

		JavaPairRDD<String, String> date2pair = date2cluster.reduceByKey(new Function2<String, String, String>() {

			@Override
			public String call(String v1, String v2) throws Exception {
				return v1 + "::" + v2;
			}
		});
		




		JavaPairRDD<String, Integer> stocks2one = date2pair.mapToPair(new PairFunction<Tuple2<String,String>, String, Integer>() {

			@Override
			public Tuple2<String, Integer> call(Tuple2<String, String> t) throws Exception {
				return new Tuple2<String,Integer>(t._2(),new Integer(1));
			}
		});
		
		JavaPairRDD<String, Integer> stocks2count = stocks2one.reduceByKey(new Function2<Integer, Integer, Integer>() {

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		
		stocks2count.sortByKey().foreach(new VoidFunction<Tuple2<String,Integer>>() {
			
			@Override
			public void call(Tuple2<String, Integer> t) throws Exception {
				if (t._1().contains("::")) {
					System.out.println(t);
				}
					
			}
		} );
		
		
		
		sc.stop();
	}

}
