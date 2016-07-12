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

import it.himyd.stock.StockCluster;
import it.himyd.stock.StockOHLC;
import it.himyd.stock.StockVariation;
import it.himyd.stock.finance.yahoo.Stock;
import scala.Tuple2;

public class AnalysisStreamingRunner implements Serializable {

	private static final long serialVersionUID = 1L;

	private Duration windowDuration;
	private Duration slideDuration;

	public AnalysisStreamingRunner() {
		this.windowDuration = Durations.seconds(60);
		this.slideDuration = Durations.seconds(10);
	}

	public AnalysisStreamingRunner(Duration windowDuration, Duration slideDuration) {
		this.windowDuration = windowDuration;
		this.slideDuration = slideDuration;
	}

	public AnalysisStreamingRunner(int windowDuration, int slideDuration) {
		this.windowDuration = Durations.seconds(windowDuration);
		this.slideDuration = Durations.seconds(slideDuration);
	}

	/* OHLC ANALYSIS */

	public JavaPairDStream<String, Double> low(JavaDStream<Stock> stocks) {
		JavaPairDStream<String, Double> min = stocks
				.mapToPair(stock -> new Tuple2<String, Double>(stock.getSymbol(),
						stock.getQuote().getPrice().doubleValue()))
				.reduceByKeyAndWindow((x, y) -> Double.valueOf(x < y ? x : y), windowDuration, slideDuration);

		return min;
	}

	public JavaPairDStream<String, Double> high(JavaDStream<Stock> stocks) {
		JavaPairDStream<String, Double> max = stocks
				.mapToPair(stock -> new Tuple2<String, Double>(stock.getSymbol(),
						stock.getQuote().getPrice().doubleValue()))
				.reduceByKeyAndWindow((x, y) -> Double.valueOf(x > y ? x : y), windowDuration, slideDuration);

		return max;
	}

	public JavaPairDStream<String, Double> open(JavaDStream<Stock> stocks) {

		JavaPairDStream<String, Double> open = stocks
				.mapToPair(stock -> new Tuple2<String, Stock>(stock.getSymbol(), stock))
				.reduceByKeyAndWindow((x,
						y) -> (x.getQuote().getLastTradeTime().compareTo(y.getQuote().getLastTradeTime()) < 0 ? x : y),
						windowDuration, slideDuration)
				.mapToPair(t -> new Tuple2<String, Double>(t._1(), t._2().getQuote().getPrice().doubleValue()));

		return open;
	}

	public JavaPairDStream<String, Double> close(JavaDStream<Stock> stocks) {
		JavaPairDStream<String, Double> close = stocks
				.mapToPair(stock -> new Tuple2<String, Stock>(stock.getSymbol(), stock))
				.reduceByKeyAndWindow((x,
						y) -> (x.getQuote().getLastTradeTime().compareTo(y.getQuote().getLastTradeTime()) > 0 ? x : y),
						windowDuration, slideDuration)
				.mapToPair(t -> new Tuple2<String, Double>(t._1(), t._2().getQuote().getPrice().doubleValue()));

		return close;
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

	public void printMostUpOld(JavaDStream<Stock> stocks, int topK) {

		JavaPairDStream<String, Double> mostUp = getSymbolAndOHLC(stocks)
				.mapToPair(x -> new Tuple2<>(x._1(), ((x._2().getClose() / x._2().getOpen()) - 1) * 100));

		mostUp.foreachRDD(
				rdd -> rdd.top(topK, (o1, o2) -> o1._2().compareTo(o2._2())).forEach(t -> System.out.println(t)));

	}

	public void printMostDownOld(JavaDStream<Stock> stocks, int topK) {

		JavaPairDStream<String, Double> mostDown = getSymbolAndOHLC(stocks)
				.mapToPair(x -> new Tuple2<>(x._1(), ((x._2().getClose() / x._2().getOpen()) - 1) * 100));

		mostDown.foreachRDD(
				rdd -> rdd.top(topK, (o1, o2) -> o1._2().compareTo(o2._2())).forEach(t -> System.out.println(t)));

	}

	public void printPriceMostUp(JavaDStream<Stock> stocks, int topK) {
		printPriceMostX(stocks, topK, false);
	}

	public void printPriceMostDown(JavaDStream<Stock> stocks, int topK) {
		printPriceMostX(stocks, topK, true);
	}

	public void printPriceMostX(JavaDStream<Stock> stocks, int topK, boolean down) {
		String message = "Top " + topK + " " + (down ? "Negative" : "Positive");

		JavaPairDStream<String, Double> most = getSymbolAndOHLC(stocks).mapToPair(x -> new Tuple2<>(x._1(),
				(x._2().getClose() == x._2().getOpen() ? (((x._2().getClose() / x._2().getOpen()) - 1) * 100) : 0.0)));

		most.mapToPair(t -> new Tuple2<Double, String>(t._2(), t._1())).foreachRDD(rdd -> {
			System.out.println(message);
			rdd.sortByKey(down).take(topK).forEach(t -> System.out.println(t._2() + " | " + t._1() + "%"));
		});

	}

	public void printVolumeMostVariation(JavaDStream<Stock> stocks, int topK) {
		printVolumeVariation(stocks, topK, false);
	}

	// volumi negativi? o cmq molti con 0
	public void printLeastVolumeVariation(JavaDStream<Stock> stocks, int topK) {
		printVolumeVariation(stocks, topK, true);
	}

	public void printVolumeVariation(JavaDStream<Stock> stocks, int topK, boolean least) {
		String message = (least ? "Bottom" : "Top") + " " + topK + " Volume Variations";

		JavaPairDStream<String, Stock> symbolStock = stocks.mapToPair(stock -> new Tuple2<>(stock.getSymbol(), stock));

		JavaPairDStream<String, Stock> open = symbolStock.reduceByKeyAndWindow(
				(x, y) -> (x.getQuote().getLastTradeTime().compareTo(y.getQuote().getLastTradeTime()) < 0 ? x : y),
				windowDuration, slideDuration);

		JavaPairDStream<String, Stock> close = symbolStock.reduceByKeyAndWindow(
				(x, y) -> (x.getQuote().getLastTradeTime().compareTo(y.getQuote().getLastTradeTime()) > 0 ? x : y),
				windowDuration, slideDuration);

		open.join(close)
				.mapToPair(
						t -> new Tuple2<String, Double>(t._1(),
								t._2()._2().getQuote().getPrice()
										.doubleValue() == t._2()._1().getQuote().getPrice()
												.doubleValue()
														? ((t._2()._2().getQuote().getPrice().doubleValue()
																/ t._2()._1().getQuote().getPrice().doubleValue() - 1)
																* 100)
														: 0.0))
				.mapToPair(t -> new Tuple2<Double, String>(t._2(), t._1())).foreachRDD(rdd -> {
					System.out.println(message);
					rdd.sortByKey(least).take(topK).forEach(t -> System.out.println(t._2() + " | " + t._1() + "%"));
				});

	}

	/* STOCK ANALYSIS */

	public JavaPairDStream<String, Double> average(JavaDStream<Stock> stocks) {
		JavaPairDStream<String, Double> avg = stocks.mapToPair(new AverageMap())
				.reduceByKeyAndWindow(new AverageReduce(), windowDuration, slideDuration).mapToPair(new AverageMap2());

		return avg;
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

	/* KAFKA CONVERTERS */

	public JavaDStream<Stock> convertKafkaJsonToStock(JavaPairInputDStream<String, String> messages) {
		JavaDStream<Stock> stocks = messages.map(line -> Stock.fromJSONString((line._2)));
		return stocks;
	}

	public JavaDStream<Stock> convertKafkaStringToStock(JavaPairInputDStream<String, String> messages) {
		JavaDStream<Stock> stocks = messages.map(line -> Stock.fromLineString((line._2)));
		return stocks;
	}

	// utilized when kafka put all data in 1 line
	public JavaDStream<Stock> convertKafkaMessagesToStockFromOneLine(JavaPairInputDStream<String, String> messages) {

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

	/* PERCENTAGE VARIATION ANALYSIS */

	public JavaPairDStream<String, StockVariation> percentageVariationPair(JavaDStream<Stock> stocks) {
		JavaPairDStream<String, StockVariation> percentageVariation = percentageVariation(stocks)
				.mapToPair(x -> new Tuple2<>(x.getName(), x));

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

	/* CLUSTER ANALYSIS */

	public JavaPairDStream<String, Integer> getSimilarStocks(JavaDStream<StockCluster> clusters) {

		JavaPairDStream<String, String> date2cluster = clusters
				.mapToPair(t -> new Tuple2<>(t.getClustertime() + "--" + t.getCluster(), t.getSymbol()));

		JavaPairDStream<String, String> date2pair = date2cluster.reduceByKeyAndWindow((v1, v2) -> v1 + "::" + v2,
				windowDuration);

		JavaPairDStream<String, Integer> stocks2one = date2pair.mapToPair(t -> new Tuple2<>(t._2(), new Integer(1)));

		JavaPairDStream<String, Integer> stocks2count = stocks2one.reduceByKeyAndWindow((v1, v2) -> v1 + v2,
				windowDuration);

		return stocks2count;
	}

	public void printSameTrendStock(JavaDStream<StockCluster> clusters, int topK) {
		getSimilarStocks(clusters).mapToPair(t -> new Tuple2<>(t._2(), t._1()))
				.foreachRDD(rdd -> rdd.sortByKey(false).take(topK).forEach(t -> System.out.println(t)));
	}

	/* GETTERS & SETTERS */

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
