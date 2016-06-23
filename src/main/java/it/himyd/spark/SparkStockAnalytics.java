package it.himyd.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import it.himyd.analysis.AnalysisRunner;
import it.himyd.finance.yahoo.Stock;
import it.himyd.kafka.KafkaConnector;
import it.himyd.spark.ml.clustering.StockClusterer;

import scala.Tuple2;

public class SparkStockAnalytics {
	private final static Duration BATCH_DURATION = Durations.seconds(1);

	private final static Duration WINDOW_DURATION = Durations.seconds(60);
	private final static Duration SLIDE_DURATION = Durations.seconds(10);

	public final static int CLUSTERING_FEATURE_NUMBER = 3;

	public static void main(String s[]) throws Exception {

		// Create a local StreamingContext with two working thread and batch interval of 1 second
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

		// JavaDStream<StockSample> sampleStocks = ar.convertStockToStockSample(stocks);
		// sampleStocks.print();

		StockClusterer kms = new StockClusterer();
		kms.clusterStocks(stocks).print();


		/*
		 * JavaPairDStream<String, Double> avg = stocks.mapToPair(new AverageMap())
		 * .reduceByKeyAndWindow(new AverageReduce(), WINDOW_DURATION, SLIDE_DURATION)
		 * .mapToPair(new AverageMap2()); // avg.print();
		 * 
		 * // JavaPairDStream<String,Double> avg = messages // .mapToPair(new AverageMap()) //
		 * .reduceByKeyAndWindow(new AverageReduce(), Durations.seconds(10), Durations.seconds(5))
		 * // .mapToPair(new AverageMap2()); // avg.print();
		 * 
		 * JavaPairDStream<String, Double> min = stocks.mapToPair(stock -> new
		 * Tuple2<>(stock.getSymbol(), stock.getQuote().getPrice().doubleValue()))
		 * .reduceByKeyAndWindow((x, y) -> Double.valueOf(x < y ? x : y), WINDOW_DURATION,
		 * SLIDE_DURATION); // min.print();
		 * 
		 * JavaPairDStream<String, Double> max = stocks.mapToPair(stock -> new
		 * Tuple2<>(stock.getSymbol(), stock.getQuote().getPrice().doubleValue()))
		 * .reduceByKeyAndWindow((x, y) -> Double.valueOf(x > y ? x : y), WINDOW_DURATION,
		 * SLIDE_DURATION); // max.print();
		 * 
		 * JavaPairDStream<String, Stock> symbolStock = stocks.mapToPair(stock -> new
		 * Tuple2<>(stock.getSymbol(), stock));
		 * 
		 * JavaPairDStream<String, Stock> newestStock = symbolStock .reduceByKeyAndWindow( (x, y) ->
		 * (x.getQuote().getLastTradeTime() .compareTo(y.getQuote().getLastTradeTime()) > 0 ? x :
		 * y), WINDOW_DURATION, SLIDE_DURATION);
		 * 
		 * JavaPairDStream<String, Stock> oldestStock = symbolStock .reduceByKeyAndWindow( (x, y) ->
		 * (x.getQuote().getLastTradeTime() .compareTo(y.getQuote().getLastTradeTime()) < 0 ? x :
		 * y), WINDOW_DURATION, SLIDE_DURATION);
		 * 
		 * JavaPairDStream<String, Tuple2<Stock, Stock>> join = newestStock.join(oldestStock);
		 * 
		 * JavaPairDStream<String, Double> percentageVariation = join.mapToPair(line -> new
		 * Tuple2<>(line._1(), line._2()._1().getQuote().getPrice().doubleValue() /
		 * line._2()._2().getQuote().getPrice().doubleValue())); percentageVariation.print();
		 */

		jssc.start();
		jssc.awaitTermination();

	}

	private static final class AverageMap implements PairFunction<Stock, String, Tuple2<Double, Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<String, Tuple2<Double, Integer>> call(Stock stock) throws Exception {
			String symbol = stock.getSymbol();
			Double price = stock.getQuote().getPrice().doubleValue();

			return new Tuple2<>(symbol, new Tuple2<>(price, new Integer(1)));
		}

	}

	private static final class AverageReduce
			implements Function2<Tuple2<Double, Integer>, Tuple2<Double, Integer>, Tuple2<Double, Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<Double, Integer> call(Tuple2<Double, Integer> i1, Tuple2<Double, Integer> i2) {
			Tuple2<Double, Integer> tuple = new Tuple2<Double, Integer>(i1._1 + i2._1, i1._2 + i2._2);

			return tuple;
		}
	}

	private static final class AverageMap2
			implements PairFunction<Tuple2<String, Tuple2<Double, Integer>>, String, Double> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<String, Double> call(Tuple2<String, Tuple2<Double, Integer>> tuple) throws Exception {
			String symbol = tuple._1;
			Double avg = tuple._2._1 / tuple._2._2;

			return new Tuple2<>(symbol, avg);
		}

	}

	// private static final class StockToPair implements PairFunction<StockPrice, String, Double> {
	// private static final long serialVersionUID = 1L;
	//
	// @Override
	// public Tuple2<String, Double> call(StockPrice stock) throws Exception {
	// String symbol = stock.getSymbol();
	// Double price = stock.getPrice();
	//
	// return new Tuple2<>(symbol, price);
	// }
	//
	// }
	//
	//
	// private static final class Sum implements Function2<Double, Double, Double> {
	// private static final long serialVersionUID = 1L;
	//
	// @Override
	// public Double call(Double i1, Double i2) {
	// return i1 + i2;
	// }
	// }
}
