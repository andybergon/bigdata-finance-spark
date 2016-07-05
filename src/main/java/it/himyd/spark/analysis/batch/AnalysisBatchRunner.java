package it.himyd.spark.analysis.batch;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import it.himyd.stock.StockCluster;
import scala.Tuple2;

public class AnalysisBatchRunner implements Serializable {

	private static final long serialVersionUID = 1L;

	public void printSimilarStocks(JavaRDD<StockCluster> clusters) {

		JavaPairRDD<String, String> date2cluster = clusters
				.mapToPair(t -> new Tuple2<>(t.getClustertime() + "--" + t.getCluster(), t.getSymbol()));

		JavaPairRDD<String, String> date2pair = date2cluster.reduceByKey((v1, v2) -> v1 + "::" + v2);

		JavaPairRDD<String, Integer> stocks2one = date2pair.mapToPair(t -> new Tuple2<>(t._2(), new Integer(1)));

		JavaPairRDD<String, Integer> stocks2count = stocks2one.reduceByKey((v1, v2) -> v1 + v2);

		stocks2count.sortByKey().foreach(t -> {
			if (t._1().contains("::")) {
				System.out.println(t);
			}

		});
	}

}
