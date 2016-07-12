package it.himyd.spark.ml.classification;

import java.io.Serializable;

import org.apache.spark.SparkConf;
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
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import it.himyd.kafka.KafkaConnector;
import it.himyd.spark.analysis.streaming.AnalysisStreamingRunner;
import it.himyd.stock.StockOHLC;
import it.himyd.stock.finance.yahoo.Stock;
import scala.Tuple2;

public class StockClassifierStreaming implements Serializable {
	private static final long serialVersionUID = 1L;

	private final static int NUM_FEATURES = 3;

	private StreamingLogisticRegressionWithSGD model; // up or not
	private Duration batchDuration;

	public StockClassifierStreaming() {
		model = new StreamingLogisticRegressionWithSGD();
		model.setStepSize(0.5);
		model.setNumIterations(10);
		model.setInitialWeights(Vectors.zeros(NUM_FEATURES));

		batchDuration = Durations.seconds(10); // default
	}

	public static void main(String[] args) {
		StockClassifierStreaming scs = new StockClassifierStreaming();
		Duration batchDuration = scs.getBatchDuration();

		SparkConf conf = new SparkConf().setAppName("SparkStreamingClassification");
		conf.setMaster("local[2]");

		JavaStreamingContext jssc = new JavaStreamingContext(conf, batchDuration);
		jssc.checkpoint("checkpoint"); // TODO: who calls this class have to do this

		KafkaConnector kc = new KafkaConnector(jssc);
		JavaPairInputDStream<String, String> messages = kc.getStream();

		System.out.println("Starting Analysis...");
		AnalysisStreamingRunner ar = new AnalysisStreamingRunner();
		ar.setSlideDuration(batchDuration);

		JavaDStream<Stock> stocks = ar.convertKafkaJsonToStock(messages);
		JavaDStream<StockOHLC> ohlc = ar.getOHLC(stocks);

		// scs.printPrecision(ohlc, 60 * 10, 10);

		scs.train(ohlc);

		JavaPairDStream<String, Vector> symbol2testData = getTestDStream(ohlc);
		// symbol2testData.print();

		JavaPairDStream<String, Double> predictions = scs.predict(symbol2testData);
		// predictions.print();

		jssc.start();
		jssc.awaitTermination();
	}

	public void train(JavaDStream<StockOHLC> ohlc) {
		JavaDStream<LabeledPoint> trainingData = getTrainingDStream(ohlc);
		trainingData.cache();

		this.getModel().trainOn(trainingData);
	}

	public JavaPairDStream<String, Double> predict(JavaPairDStream<String, Vector> symbol2testData) {
		return this.getModel().predictOnValues(symbol2testData);
	}

	public static JavaDStream<LabeledPoint> getTrainingDStream(JavaDStream<StockOHLC> ohlc) {
		return convertOHLCtoSymbol2Training(ohlc).map(x -> x._2());
	}

	public static JavaPairDStream<String, LabeledPoint> convertOHLCtoSymbol2Training(JavaDStream<StockOHLC> ohlc) {

		return ohlc.mapToPair(new PairFunction<StockOHLC, String, LabeledPoint>() {
			private static final long serialVersionUID = 1L;
			Double scalingFactor = new Double(1);

			@Override
			public Tuple2<String, LabeledPoint> call(StockOHLC stock) throws Exception {
				Double label;
				Double percOL = stock.getOpen() / stock.getLow();
				percOL = (percOL - 1) * 100 * scalingFactor;
				Double percOH = stock.getOpen() / stock.getHigh();
				percOH = (percOH - 1) * 100 * scalingFactor;
				Double percHL = stock.getHigh() / stock.getLow();
				percHL = (percHL - 1) * 100 * scalingFactor;
				Double percCO = stock.getClose() / stock.getOpen();
				percCO = (percCO - 1) * 100;

				double vc[] = new double[NUM_FEATURES];
				vc[0] = percOL;
				vc[1] = percOH;
				vc[2] = percHL;

				if ((percOL == 0) && (percOH == 0) && (percHL == 0)) {
					// if classifier UP/DOWN
					label = Math.random() > 0.5 ? new Double(1) : new Double(0);
					// if classifier UP/NOT UP
					// label = new Double(0);
				} else {
					if (percCO <= 0) {
						label = new Double(0);
					} else {
						label = new Double(1);
					}
				}

				return new Tuple2<>(stock.getSymbol(), new LabeledPoint(label, Vectors.dense(vc)));
			}

		});
	}

	public static JavaPairDStream<String, Vector> getTestDStream(JavaDStream<StockOHLC> stocks) {

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

				double vc[] = new double[NUM_FEATURES];
				vc[0] = percOL;
				vc[1] = percOH;
				vc[2] = percHL;

				return new Tuple2<String, Vector>(stock.getSymbol(), Vectors.dense(vc));
			}

		});

	}

	public void printPrecision(JavaDStream<StockOHLC> ohlc, int windowSeconds, int printRateSeconds) {
		Duration windowDuration = Durations.seconds(windowSeconds);
		Duration printRate = Durations.seconds(printRateSeconds);

		JavaPairDStream<String, LabeledPoint> labeledData = convertOHLCtoSymbol2Training(ohlc);

		// (symbol,(predicted,correct))
		JavaPairDStream<String, Tuple2<Double, Double>> predictionCorrect = labeledData
				.mapToPair(new PairFunction<Tuple2<String, LabeledPoint>, String, Tuple2<Double, Double>>() {

					@Override
					public Tuple2<String, Tuple2<Double, Double>> call(Tuple2<String, LabeledPoint> t)
							throws Exception {
						String symbol = t._1();
						Double prediction = model.latestModel().predict(t._2().features());
						Double correct = t._2().label();
						Tuple2<Double, Double> predictionCorrect = new Tuple2<Double, Double>(prediction, correct);

						return new Tuple2<String, Tuple2<Double, Double>>(symbol, predictionCorrect);
					}

				});

		// predictionCorrect.print();

		// (symbol,(#correct,#error))
		JavaPairDStream<String, Tuple2<Double, Double>> correctOrNot = predictionCorrect
				.mapToPair(new PairFunction<Tuple2<String, Tuple2<Double, Double>>, String, Tuple2<Double, Double>>() {

					@Override
					public Tuple2<String, Tuple2<Double, Double>> call(Tuple2<String, Tuple2<Double, Double>> t)
							throws Exception {
						boolean isCorrect = t._2()._1().equals(t._2()._2());
						Tuple2<Double, Double> correct = isCorrect ? new Tuple2<>(new Double(1), new Double(0))
								: new Tuple2<>(new Double(0), new Double(1));

						return new Tuple2<String, Tuple2<Double, Double>>(t._1(), correct);
					}
				});

		// correctOrNot.print();

		// (symbol,(#correct,#error))
		JavaPairDStream<String, Tuple2<Double, Double>> correctOrNotCount = correctOrNot.reduceByKeyAndWindow(
				new Function2<Tuple2<Double, Double>, Tuple2<Double, Double>, Tuple2<Double, Double>>() {

					@Override
					public Tuple2<Double, Double> call(Tuple2<Double, Double> v1, Tuple2<Double, Double> v2)
							throws Exception {
						return new Tuple2<>(new Double(v1._1() + v2._1()), new Double(v1._2() + v2._2()));
					}
				}, windowDuration, printRate);

		// correctOrNotCount.print();

		// (symbol,correct%)
		JavaPairDStream<String, Double> symbolPercentage = correctOrNotCount
				.mapToPair(x -> new Tuple2<>(x._1(), 100 * (x._2()._1()) / (x._2()._1() + x._2()._2())));

		symbolPercentage.print();

		JavaDStream<Double> percentage = correctOrNotCount.map(x -> x._2()).reduceByWindow(
				new Function2<Tuple2<Double, Double>, Tuple2<Double, Double>, Tuple2<Double, Double>>() {

					@Override
					public Tuple2<Double, Double> call(Tuple2<Double, Double> v1, Tuple2<Double, Double> v2)
							throws Exception {
						return new Tuple2<Double, Double>(v1._1() + v2._2(), v1._2() + v2._2);
					}
				}, windowDuration, printRate).map(x -> new Double(x._1() / (x._1() + x._2())));

		percentage.print(); // not working
	}

	public StreamingLogisticRegressionWithSGD getModel() {
		return this.model;
	}

	public void setSlrModel(StreamingLogisticRegressionWithSGD model) {
		this.model = model;
	}

	public Duration getBatchDuration() {
		return batchDuration;
	}

	public void setBatchDuration(Duration batchDuration) {
		this.batchDuration = batchDuration;
	}

}
