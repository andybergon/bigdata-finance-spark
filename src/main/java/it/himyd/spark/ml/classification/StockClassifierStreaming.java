package it.himyd.spark.ml.classification;

import java.util.HashMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.StreamingLogisticRegressionWithSGD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import org.apache.spark.api.java.function.PairFunction;

import it.himyd.kafka.KafkaConnector;
import it.himyd.spark.analysis.streaming.AnalysisRunner;
import it.himyd.stock.StockOHLC;
import it.himyd.stock.finance.yahoo.Stock;
import scala.Tuple2;

public class StockClassifierStreaming {
	private final static Duration BATCH_DURATION = Durations.seconds(10);
	private final static int NUM_FEATURES = 2;
	private final static int NUM_FEATURES_ACT = 3;

	private StreamingLogisticRegressionWithSGD model;

	public StockClassifierStreaming() {
		model = new StreamingLogisticRegressionWithSGD();
		model.setStepSize(0.5);
		model.setNumIterations(10);
		model.setInitialWeights(Vectors.zeros(NUM_FEATURES_ACT));
	}

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SparkStockAnalytics");
		conf.setMaster("local[2]");

		JavaStreamingContext jssc = new JavaStreamingContext(conf, BATCH_DURATION);

		// RIPORTA
		jssc.checkpoint("checkpoint");

		KafkaConnector kc = new KafkaConnector(jssc);
		JavaPairInputDStream<String, String> messages = kc.getStream();
		// messages.print();

		System.out.println("Starting Analysis...");
		AnalysisRunner ar = new AnalysisRunner();
		ar.setSlideDuration(BATCH_DURATION);
		JavaDStream<Stock> stocks = ar.convertKafkaMessagesToStock(messages);

		JavaDStream<StockOHLC> ohlc = ar.getOHLC(stocks);
		// ohlc.print();

		StockClassifierStreaming classifier = new StockClassifierStreaming();
		JavaDStream<LabeledPoint> trainingData = getDStreamTrainingActual(ohlc);
		// trainingData.print();
		trainingData.cache();
		classifier.getModel().trainOn(trainingData);

		JavaPairDStream<String, Vector> testData = getDStreamTestActual(ohlc);
		// testData.print();

		JavaPairDStream<String, Double> predictions = classifier.getModel().predictOnValues(testData);
		predictions.print();

		jssc.start();
		jssc.awaitTermination();
	}

	public static JavaDStream<LabeledPoint> getDStreamTrainingActual(JavaDStream<StockOHLC> ohlc) {

		return ohlc.map(new Function<StockOHLC, LabeledPoint>() {
			private static final long serialVersionUID = 1L;
			Double scalingFactor = new Double(1);

			@Override
			public LabeledPoint call(StockOHLC stock) throws Exception {
				Double label;
				Double percOL = stock.getOpen() / stock.getLow();
				percOL = (percOL - 1) * 100 * scalingFactor;
				Double percOH = stock.getOpen() / stock.getHigh();
				percOH = (percOH - 1) * 100 * scalingFactor;
				Double percHL = stock.getHigh() / stock.getLow();
				percHL = (percHL - 1) * 100 * scalingFactor;
				Double percCO = stock.getClose() / stock.getOpen();
				percCO = (percCO - 1) * 100;

				double vc[] = new double[NUM_FEATURES_ACT];
				vc[0] = percOL;
				vc[1] = percOH;
				vc[2] = percHL;

				if ((percOL == 0) && (percOH == 0) && (percHL == 0)) {
					label = Math.random() > 0.5 ? new Double(1) : new Double(0);
				} else {
					if (percCO <= 0) {
						label = new Double(0);
					} else {
						label = new Double(1);
					}
				}

				return new LabeledPoint(label, Vectors.dense(vc));
			}

		});
	}

	public static JavaPairDStream<String, Vector> getDStreamTestActual(JavaDStream<StockOHLC> stocks) {

		return stocks.mapToPair(new PairFunction<StockOHLC, String, Vector>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Vector> call(StockOHLC stock) throws Exception {
				Double percOL = stock.getOpen() / stock.getLow();
				percOL = (percOL - 1) * 100;
				Double percOH = stock.getOpen() / stock.getHigh();
				percOH = (percOH - 1) * 100;
				Double percHL = stock.getHigh() / stock.getLow();
				percHL = (percHL - 1) * 100;

				double vc[] = new double[NUM_FEATURES_ACT];
				vc[0] = percOL;
				vc[1] = percOH;
				vc[2] = percHL;

				return new Tuple2<String, Vector>(stock.getSymbol(), Vectors.dense(vc));
			}

		});

	}

	public static JavaDStream<LabeledPoint> getDStreamTraining(JavaDStream<StockOHLC> ohlc) {

		// TODO: check what map to use, java/scala mutable/scala immutable
		HashMap<String, StockOHLC> symbol2prevStock = new HashMap<String, StockOHLC>(); // scala mutable
		HashMap<String, StockOHLC> symbol2currStock = new HashMap<String, StockOHLC>();

		return ohlc.map(new Function<StockOHLC, LabeledPoint>() {
			private static final long serialVersionUID = 1L;

			@Override
			public LabeledPoint call(StockOHLC stock) throws Exception {
				Double label, prevCO, prevHL, currCO;
				StockOHLC prevStock = null;
				System.out.println(symbol2currStock.keySet());

				if (symbol2currStock.containsKey(stock.getSymbol())) {
					System.out.println(stock.getSymbol());
					prevStock = symbol2currStock.get(stock.getSymbol());
					symbol2prevStock.put(stock.getSymbol(), prevStock);
				}

				symbol2currStock.put(stock.getSymbol(), stock);
				// System.out.println(symbol2currStock.keySet());

				if (symbol2prevStock.containsKey(stock.getSymbol())) {
					System.out.println(stock.getSymbol());

					StockOHLC currStock = stock;

					prevCO = prevStock.getClose() / prevStock.getOpen();
					prevCO = (prevCO - 1) * 100;

					prevHL = prevStock.getHigh() / prevStock.getLow();
					prevHL = (prevHL - 1) * 100;

					double vc[] = new double[NUM_FEATURES];
					vc[0] = prevCO;
					vc[1] = prevHL;

					currCO = currStock.getClose() / currStock.getOpen();
					currCO = (currCO - 1) * 100;

					// for now: only up and down. TODO: remain or exact value
					// can't have 3 cases with this model
					if (currCO <= 0) {
						label = 0.0; // down
					} else {
						label = 1.0; // up
					}

					System.out.println(currStock.getTradetime().getTime() + " - symbol: " + currStock.getSymbol()
							+ ", prevCO: " + prevCO + ", currCO: " + currCO);

					return new LabeledPoint(label, Vectors.dense(vc));
				} else {
					return new LabeledPoint(0, Vectors.dense(new double[] { 0 }));
				}

			}

		});
	}

	public static JavaDStream<Vector> getDStreamTest(JavaDStream<Stock> stocks) {

		return stocks.map(new Function<Stock, Vector>() {

			@Override
			public Vector call(Stock stock) throws Exception {

				return null;
			}

		});

	}

	public StreamingLogisticRegressionWithSGD getModel() {
		return this.model;
	}

	public void setSlrModel(StreamingLogisticRegressionWithSGD model) {
		this.model = model;
	}

}
