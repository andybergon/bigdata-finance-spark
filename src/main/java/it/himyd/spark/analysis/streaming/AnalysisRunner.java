package it.himyd.spark.analysis.streaming;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import it.himyd.stock.StockOHLC;
import it.himyd.stock.StockSample;
import it.himyd.stock.StockVariation;
import it.himyd.stock.finance.yahoo.Stock;
import scala.Tuple2;

public class AnalysisRunner implements Serializable {

	private static final long serialVersionUID = 1L;

	private Duration windowDuration;
	private Duration slideDuration;

	public AnalysisRunner() {
		this.windowDuration = Durations.seconds(60);
		this.slideDuration = Durations.seconds(10);
	}

	public JavaPairDStream<String, Double> average(JavaDStream<Stock> stocks) {
		JavaPairDStream<String, Double> avg = stocks.mapToPair(new AverageMap())
				.reduceByKeyAndWindow(new AverageReduce(), windowDuration, slideDuration).mapToPair(new AverageMap2());

		return avg;
	}

	public JavaPairDStream<String, Double> low(JavaDStream<Stock> stocks) {
		JavaPairDStream<String, Double> min = stocks
				.mapToPair(stock -> new Tuple2<>(stock.getSymbol(), stock.getQuote().getPrice().doubleValue()))
				.reduceByKeyAndWindow((x, y) -> Double.valueOf(x < y ? x : y), windowDuration, slideDuration);

		return min;
	}

	public JavaPairDStream<String, Double> high(JavaDStream<Stock> stocks) {
		JavaPairDStream<String, Double> max = stocks
				.mapToPair(stock -> new Tuple2<>(stock.getSymbol(), stock.getQuote().getPrice().doubleValue()))
				.reduceByKeyAndWindow((x, y) -> Double.valueOf(x > y ? x : y), windowDuration, slideDuration);

		return max;
	}

	public JavaPairDStream<String, Double> open(JavaDStream<Stock> stocks) {
		JavaPairDStream<String, Stock> symbolStock = stocks.mapToPair(stock -> new Tuple2<>(stock.getSymbol(), stock));

		JavaPairDStream<String, Stock> openStock = symbolStock.reduceByKeyAndWindow(
				(x, y) -> (x.getQuote().getLastTradeTime().compareTo(y.getQuote().getLastTradeTime()) < 0 ? x : y),
				windowDuration, slideDuration);

		JavaPairDStream<String, Double> open = openStock
				.mapToPair(new PairFunction<Tuple2<String, Stock>, String, Double>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Double> call(Tuple2<String, Stock> t) throws Exception {
						return new Tuple2<String, Double>(t._1(), t._2().getQuote().getPrice().doubleValue());
					}
				});

		return open;
	}

	public JavaPairDStream<String, Double> close(JavaDStream<Stock> stocks) {
		JavaPairDStream<String, Stock> symbolStock = stocks.mapToPair(stock -> new Tuple2<>(stock.getSymbol(), stock));

		JavaPairDStream<String, Stock> closeStock = symbolStock.reduceByKeyAndWindow(
				(x, y) -> (x.getQuote().getLastTradeTime().compareTo(y.getQuote().getLastTradeTime()) > 0 ? x : y),
				windowDuration, slideDuration);

		JavaPairDStream<String, Double> close = closeStock
				.mapToPair(new PairFunction<Tuple2<String, Stock>, String, Double>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Double> call(Tuple2<String, Stock> t) throws Exception {
						return new Tuple2<String, Double>(t._1(), t._2().getQuote().getPrice().doubleValue());
					}
				});

		return close;
	}

	// TODO: split into methods
	public JavaPairDStream<String, Double> percentagePriceVariation(JavaDStream<Stock> stocks) {
		JavaPairDStream<String, Stock> symbolStock = stocks.mapToPair(stock -> new Tuple2<>(stock.getSymbol(), stock));

		JavaPairDStream<String, Stock> newestStock = symbolStock.reduceByKeyAndWindow(
				(x, y) -> (x.getQuote().getLastTradeTime().compareTo(y.getQuote().getLastTradeTime()) > 0 ? x : y),
				windowDuration, slideDuration);

		JavaPairDStream<String, Stock> oldestStock = symbolStock.reduceByKeyAndWindow(
				(x, y) -> (x.getQuote().getLastTradeTime().compareTo(y.getQuote().getLastTradeTime()) < 0 ? x : y),
				windowDuration, slideDuration);

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

	public JavaPairDStream<String, StockVariation> percentageVariationPair(JavaDStream<Stock> stocks) {
		JavaPairDStream<String, Stock> symbolStock = stocks.mapToPair(stock -> new Tuple2<>(stock.getSymbol(), stock));

		JavaPairDStream<String, Stock> newestStock = symbolStock.reduceByKeyAndWindow(
				(x, y) -> (x.getQuote().getLastTradeTime().compareTo(y.getQuote().getLastTradeTime()) > 0 ? x : y),
				windowDuration, slideDuration);

		JavaPairDStream<String, Stock> oldestStock = symbolStock.reduceByKeyAndWindow(
				(x, y) -> (x.getQuote().getLastTradeTime().compareTo(y.getQuote().getLastTradeTime()) < 0 ? x : y),
				windowDuration, slideDuration);

		JavaPairDStream<String, Tuple2<Stock, Stock>> join = newestStock.join(oldestStock);

		JavaPairDStream<String, StockVariation> percentageVariation = join
				.mapToPair(new PairFunction<Tuple2<String, Tuple2<Stock, Stock>>, String, StockVariation>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, StockVariation> call(Tuple2<String, Tuple2<Stock, Stock>> t)
							throws Exception {
						String name;
						Date time;
						Double priceVariation;
						Double volumeVariation;

						name = t._1();
						time = new Date(); // t._2()._1().getQuote().getLastTradeTime().getTime();
						priceVariation = t._2()._1().getQuote().getPrice().doubleValue()
								/ t._2()._2().getQuote().getPrice().doubleValue();
						volumeVariation = (double) (t._2()._1().getQuote().getVolume()
								/ t._2()._2().getQuote().getVolume());

						priceVariation = priceVariation - 1;
						volumeVariation = volumeVariation - 1;

						// to percentage, correct?
						priceVariation = priceVariation * 100;
						volumeVariation = volumeVariation * 100;

						if (!volumeVariation.equals(new Double(0))) {
							System.out.println("1, newest: " + t._2()._1().getQuote().getVolume());
							System.out.println("2, oldest: " + t._2()._2().getQuote().getVolume());
						}

						StockVariation sv = new StockVariation(name, time, priceVariation, volumeVariation);

						return new Tuple2<String, StockVariation>(sv.getName(), sv);
					}

				});

		return percentageVariation;

	}

	public JavaDStream<StockVariation> percentageVariation(JavaDStream<Stock> stocks) {
		JavaPairDStream<String, Stock> symbolStock = stocks.mapToPair(stock -> new Tuple2<>(stock.getSymbol(), stock));

		JavaPairDStream<String, Stock> newestStock = symbolStock.reduceByKeyAndWindow(
				(x, y) -> (x.getQuote().getLastTradeTime().compareTo(y.getQuote().getLastTradeTime()) > 0 ? x : y),
				windowDuration, slideDuration);

		JavaPairDStream<String, Stock> oldestStock = symbolStock.reduceByKeyAndWindow(
				(x, y) -> (x.getQuote().getLastTradeTime().compareTo(y.getQuote().getLastTradeTime()) < 0 ? x : y),
				windowDuration, slideDuration);

		JavaPairDStream<String, Tuple2<Stock, Stock>> join = newestStock.join(oldestStock);

		JavaDStream<StockVariation> percentageVariation = join
				.map(new Function<Tuple2<String, Tuple2<Stock, Stock>>, StockVariation>() {

					private static final long serialVersionUID = 1L;

					@Override
					public StockVariation call(Tuple2<String, Tuple2<Stock, Stock>> t) throws Exception {
						String name;
						Date time;
						Double priceVariation;
						Double volumeVariation;

						name = t._1();
						time = t._2()._1().getQuote().getLastTradeTime().getTime();
						priceVariation = t._2()._1().getQuote().getPrice().doubleValue()
								/ t._2()._2().getQuote().getPrice().doubleValue();
						volumeVariation = (double) (t._2()._1().getQuote().getVolume()
								/ t._2()._2().getQuote().getVolume());

						priceVariation = priceVariation - 1;
						volumeVariation = volumeVariation - 1;

						StockVariation sv = new StockVariation(name, time, priceVariation, volumeVariation);

						return sv;
					}

				});

		return percentageVariation;

	}

	public JavaDStream<StockOHLC> getOHLC(JavaDStream<Stock> stocks) {
		return getSymbolAndOHLC(stocks).map(v1 -> v1._2());
	}

	public JavaPairDStream<String, StockOHLC> getSymbolAndOHLC(JavaDStream<Stock> stocks) {
		JavaPairDStream<String, Stock> symbolStock = stocks.mapToPair(stock -> new Tuple2<>(stock.getSymbol(), stock));

		JavaPairDStream<String, Stock> low = symbolStock.reduceByKeyAndWindow(
				(x, y) -> (x.getQuote().getPrice().compareTo(y.getQuote().getPrice()) < 0 ? x : y), windowDuration,
				slideDuration);

		JavaPairDStream<String, Stock> high = symbolStock.reduceByKeyAndWindow(
				(x, y) -> (x.getQuote().getPrice().compareTo(y.getQuote().getPrice()) > 0 ? x : y), windowDuration,
				slideDuration);

		JavaPairDStream<String, Stock> open = symbolStock.reduceByKeyAndWindow(
				(x, y) -> (x.getQuote().getLastTradeTime().compareTo(y.getQuote().getLastTradeTime()) < 0 ? x : y),
				windowDuration, slideDuration);

		JavaPairDStream<String, Stock> close = symbolStock.reduceByKeyAndWindow(
				(x, y) -> (x.getQuote().getLastTradeTime().compareTo(y.getQuote().getLastTradeTime()) > 0 ? x : y),
				windowDuration, slideDuration);

		JavaPairDStream<String, Tuple2<Tuple2<Tuple2<Stock, Stock>, Stock>, Stock>> join = open.join(high).join(low)
				.join(close);

		JavaPairDStream<String, StockOHLC> ohlc = join.mapToPair(
				new PairFunction<Tuple2<String, Tuple2<Tuple2<Tuple2<Stock, Stock>, Stock>, Stock>>, String, StockOHLC>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, StockOHLC> call(
							Tuple2<String, Tuple2<Tuple2<Tuple2<Stock, Stock>, Stock>, Stock>> t) throws Exception {

						StockOHLC stockOHLC = new StockOHLC();
						stockOHLC.setSymbol(t._1());
						// stockOHLC.setTradeTime(t._2()._2().getQuote().getLastTradeTime());
						stockOHLC.setOpen(t._2()._1()._1()._1().getQuote().getPrice().doubleValue());
						stockOHLC.setHigh(t._2()._1()._1()._2().getQuote().getPrice().doubleValue());
						stockOHLC.setLow(t._2()._1()._2().getQuote().getPrice().doubleValue());
						stockOHLC.setClose(t._2()._2().getQuote().getPrice().doubleValue());
						// or
						// long startVolume = t._2()._2().getQuote().getVolume();
						// long endVolume = t._2()._1()._1()._1().getQuote().getVolume();
						// stockOHLC.setVolume(endVolume - startVolume);
						stockOHLC.setVolume(t._2()._2().getQuote().getVolume());

						return new Tuple2<String, StockOHLC>(t._1(), stockOHLC);
					}
				});

		return ohlc;
	}

	public Duration getWindowDuration() {
		return windowDuration;
	}

	public void setWindowDuration(Duration windowDuration) {
		this.windowDuration = windowDuration;
	}

	public Duration getSlideDuration() {
		return slideDuration;
	}

	public void setSlideDuration(Duration slideDuration) {
		this.slideDuration = slideDuration;
	}

}
