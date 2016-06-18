package it.himyd.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.clustering.StreamingKMeans;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import org.apache.spark.streaming.kafka.KafkaUtils;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.streaming.*;
import com.datastax.spark.connector.writer.RowWriterFactory;

import com.datastax.spark.connector.japi.CassandraRow;
import static com.datastax.spark.connector.japi.CassandraStreamingJavaUtil.*;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import it.himyd.finance.yahoo.Stock;
import it.himyd.spark.kafka.KafkaConnector;
import it.himyd.spark.kmeans.KMeansExample;
import it.himyd.spark.kmeans.KMeansStreaming;
import it.himyd.stock.StockSample;
import kafka.serializer.StringDecoder;

import scala.Tuple2;

public class SparkStockAnalytics {
	private final static Duration BATCH_DURATION = Durations.seconds(1);

	private final static Duration WINDOW_DURATION = Durations.seconds(60);
	private final static Duration SLIDE_DURATION = Durations.seconds(10);

	public static void main(String s[]) throws Exception {

		// Create a local StreamingContext with two working thread and batch interval of 1 second
		SparkConf conf = new SparkConf().setAppName("SparkStockAnalytics");
		conf.setMaster("local[2]");
		conf.set("spark.cassandra.connection.host", "localhost");

		JavaStreamingContext jssc = new JavaStreamingContext(conf, BATCH_DURATION);

		KafkaConnector kc = new KafkaConnector(jssc);
		JavaPairInputDStream<String, String> messages = kc.getStream();



		// KMeansExample km = new KMeansExample(jssc);

		// messages.print();
		// messages.foreach(System.out.println(line));

		// messages.foreachRDD(rdd -> {
		// System.out.println("--- New RDD with " + rdd.partitions().size()
		// + " partitions and " + rdd.count() + " records");
		// rdd.foreach(record -> System.out.println(record._2));
		// });

		// ObjectMapper mapper = new ObjectMapper();

		// JavaDStream<Stock> messagesSplitted = messages.flatMap(new FlatMapFunction<Tuple2<String,
		// String>, Stock>() {
		// private static final long serialVersionUID = 1L;
		//
		// @Override
		// public Iterable<Stock> call(Tuple2<String, String> t) throws Exception {
		// Map<String, String> map = mapper.readValue(t._2(), new
		// TypeReference<Map<String,Object>>(){});
		// List<Stock> stocks = new ArrayList<Stock>();
		// for(String stockString : map.values()) {
		// stocks.add(Stock.fromJSONString(stockString));
		// }
		//
		// return stocks;
		// }
		// });
		// messagesSplitted.print();



		JavaDStream<Stock> stocks = messages.map(line -> Stock.fromJSONString((line._2)));
		stocks.print();

		JavaDStream<StockSample> sampleStocks = stocks.map(stock -> new StockSample(stock));


		JavaDStream<Vector> trainingData = sampleStocks.map(new Function<StockSample, Vector>() {
			private static final long serialVersionUID = 1L;

			public Vector call(StockSample s) {
				double[] values = new double[2];
				values[0] = s.getPrice();
				values[1] = s.getTrade_timestamp().getTimezoneOffset();
				return Vectors.dense(values);
			}

		});

		trainingData.cache();
		// System.out.println("train " + trainingData.count());
		
	    JavaDStream<LabeledPoint> testData = sampleStocks.map(new Function<StockSample, LabeledPoint>() {
			public LabeledPoint call(StockSample s) throws Exception {
				double[] values = new double[2];
				values[0] = s.getPrice();
				values[1] = s.getTrade_timestamp().getTimezoneOffset();
				return new LabeledPoint(1.0, Vectors.dense(values));
			}
		});

		StreamingKMeans model = new StreamingKMeans();
		model.setK(2);
		// with a=1 all data will be used from the beginning
		// with a=0 only the most recent data will be used
		model.setDecayFactor(1.0);
		model.setRandomCenters(2, 0.0, 0L); // che valori passare?
		model.trainOn(trainingData);


		model.predictOnValues(testData.mapToPair(new PairFunction<LabeledPoint, Double, Vector>() {
			private static final long serialVersionUID = 1L;

			public Tuple2<Double, Vector> call(LabeledPoint arg0) throws Exception {
				return new Tuple2<Double, Vector>(arg0.label(), arg0.features());
			}
		})).print();


		// KMeansStreaming kms = new KMeansStreaming(jssc);
		// kms.clusterStocks(sampleStocks);


		/* start cassandra working */
		// javaFunctions(sampleStocks).writerBuilder("finance", "stocks",
		// mapToRow(StockSample.class))
		// .saveToCassandra();

		/* end cassandra working */



		// JavaRDD<StockSample> sampleStockRDD = sampleStocks.compute(new Time(60*1000));
		// javaFunctions(sampleStockRDD).writerBuilder("finance", "stocks",
		// mapToRow(StockSample.class)).saveToCassandra();

		// CassandraJavaUtil.mapToRow(StockSample.class)
		// RowWriterFactory<StockSample> rowWriterFactory
		// javaFunctions(sampleStocks).writerBuilder("finance", "stocks",
		// rowWriterFactory).saveToCassandra();

		// javaFunctions(sampleStockRDD, StockSample.class);

		// JavaRDD<String> cassandraRowsRDD = javaFunctions(jssc).cassandraTable("finance",
		// "stocks").map(new Function<CassandraRow, String>() {
		// @Override
		// public String call(CassandraRow cassandraRow) throws Exception {
		// return cassandraRow.toString();
		// }
		// });

		// StringUtils.join(cassandraRowsRDD.take(1), "\n")
		// System.out.println("Data as CassandraRows: \n" + cassandraRowsRDD.count());



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
