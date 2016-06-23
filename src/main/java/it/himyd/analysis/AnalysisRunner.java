package it.himyd.analysis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import it.himyd.finance.yahoo.Stock;
import it.himyd.stock.StockSample;
import scala.Tuple2;

public class AnalysisRunner {
	private final static Duration WINDOW_DURATION = Durations.seconds(60);
	private final static Duration SLIDE_DURATION = Durations.seconds(10);

	public JavaPairDStream<String, Double> average(JavaDStream<Stock> stocks) {
		JavaPairDStream<String, Double> avg = stocks.mapToPair(new AverageMap())
				.reduceByKeyAndWindow(new AverageReduce(), WINDOW_DURATION, SLIDE_DURATION)
				.mapToPair(new AverageMap2());

		return avg;
	}

	public JavaPairDStream<String, Double> minimum(JavaDStream<Stock> stocks) {
		JavaPairDStream<String, Double> min = stocks
				.mapToPair(stock -> new Tuple2<>(stock.getSymbol(), stock.getQuote().getPrice().doubleValue()))
				.reduceByKeyAndWindow((x, y) -> Double.valueOf(x < y ? x : y), WINDOW_DURATION, SLIDE_DURATION);

		return min;
	}

	public JavaPairDStream<String, Double> maximum(JavaDStream<Stock> stocks) {
		JavaPairDStream<String, Double> max = stocks
				.mapToPair(stock -> new Tuple2<>(stock.getSymbol(), stock.getQuote().getPrice().doubleValue()))
				.reduceByKeyAndWindow((x, y) -> Double.valueOf(x > y ? x : y), WINDOW_DURATION, SLIDE_DURATION);

		return max;
	}

	// TODO: split into methods
	public JavaPairDStream<String, Double> percentagePriceVariation(JavaDStream<Stock> stocks) {
		JavaPairDStream<String, Stock> symbolStock = stocks.mapToPair(stock -> new Tuple2<>(stock.getSymbol(), stock));

		JavaPairDStream<String, Stock> newestStock = symbolStock.reduceByKeyAndWindow(
				(x, y) -> (x.getQuote().getLastTradeTime().compareTo(y.getQuote().getLastTradeTime()) > 0 ? x : y),
				WINDOW_DURATION, SLIDE_DURATION);

		JavaPairDStream<String, Stock> oldestStock = symbolStock.reduceByKeyAndWindow(
				(x, y) -> (x.getQuote().getLastTradeTime().compareTo(y.getQuote().getLastTradeTime()) < 0 ? x : y),
				WINDOW_DURATION, SLIDE_DURATION);

		JavaPairDStream<String, Tuple2<Stock, Stock>> join = newestStock.join(oldestStock);

		JavaPairDStream<String, Double> percentageVariation = join
				.mapToPair(line -> new Tuple2<>(line._1(), line._2()._1().getQuote().getPrice().doubleValue()
						/ line._2()._2().getQuote().getPrice().doubleValue()));

		return percentageVariation;
	}

	/* SUPPORT */

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

	// private static final class StockToPair implements
	// PairFunction<StockPrice, String, Double> {
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
	// private static final class Sum implements Function2<Double, Double,
	// Double> {
	// private static final long serialVersionUID = 1L;
	//
	// @Override
	// public Double call(Double i1, Double i2) {
	// return i1 + i2;
	// }
	// }

	/* CONVERTERS */

	public JavaDStream<Stock> convertKafkaMessagesToStock(JavaPairInputDStream<String, String> messages) {
		JavaDStream<Stock> stocks = messages.map(line -> Stock.fromJSONString((line._2)));
		return stocks;
	}

	public JavaDStream<StockSample> convertStockToStockSample(JavaDStream<Stock> stocks) {
		JavaDStream<StockSample> stockSample = stocks.map(stock -> new StockSample(stock));
		return stockSample;
	}

	// utilized before when kafka put all data in 1 line
	public JavaDStream<Stock> convertKafkaMessagesToStockOLD(JavaPairInputDStream<String, String> messages) {

		ObjectMapper mapper = new ObjectMapper();
		JavaDStream<Stock> messagesSplitted = messages.flatMap(new FlatMapFunction<Tuple2<String, String>, Stock>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Stock> call(Tuple2<String, String> t) throws Exception {
				Map<String, String> map = mapper.readValue(t._2(), new TypeReference<Map<String, Object>>() {
				});
				List<Stock> stocksList = new ArrayList<Stock>();

				for (String stockString : map.values()) {
					stocksList.add(Stock.fromJSONString(stockString));
				}

				return stocksList;
			}

		});

		return messagesSplitted;
	}

}
