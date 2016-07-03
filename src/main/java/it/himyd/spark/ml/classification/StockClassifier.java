package it.himyd.spark.ml.classification;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.classification.StreamingLogisticRegressionWithSGD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;
import org.apache.spark.streaming.dstream.MapWithStateDStream;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import scala.Option;
import scala.Tuple2;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.util.ManualClock;

import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.Function4;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;

import it.himyd.analysis.AnalysisRunner;
import it.himyd.finance.yahoo.Stock;
import it.himyd.kafka.KafkaConnector;
import it.himyd.spark.ml.classification.training.OHLCstock;
import it.himyd.stock.StockVariation;
import kafka.producer.OldProducer;

public class StockClassifier {
	private final static Duration BATCH_DURATION = Durations.seconds(5);
	private final static String TRAINING_PATH = "target/classes/ml/BANKNIFTY.txt";

	private static SVMModel svmModel;
	private static StreamingLogisticRegressionWithSGD slrModel;

	private static int numFeatures = 2;

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SparkStockAnalytics");
		conf.setMaster("local[2]");

		JavaSparkContext jsc = new JavaSparkContext(conf);
		// JavaStreamingContext jssc = new JavaStreamingContext(conf,
		// BATCH_DURATION);

		// RIPORTA
		// jssc.checkpoint("~/spark-checkpoint");

		// KafkaConnector kc = new KafkaConnector(jssc);
		// JavaPairInputDStream<String, String> messages = kc.getStream();
		// messages.print();

		// AnalysisRunner ar = new AnalysisRunner();
		// JavaDStream<Stock> stocks = ar.convertKafkaMessagesToStock(messages);
		// JavaDStream<StockVariation> variation =
		// ar.percentageVariation(stocks);
		// variation.print();

		// JavaPairDStream<String, StockVariation> variationPair =
		// ar.percentageVariationPair(stocks);
		// variationPair.print();

		svmModel = new SVMWithSGD();
		trainOnIndianStock(jsc);

		// slrModel = new StreamingLogisticRegressionWithSGD();
		// slrModel.setStepSize(0.5);
		// slrModel.setNumIterations(10);
		// slrModel.setInitialWeights(Vectors.zeros(numFeatures));

		// JavaDStream<LabeledPoint> testData = getDStreamTraining(variation);
		//
		// slrModel.predictOn(getDStreamPrediction(testData)).print();

		// jssc.start();
		// jssc.awaitTermination();
	}

	private static void trainOnIndianStock(JavaSparkContext jsc) {
		
		JavaRDD<String> csv = jsc.textFile(TRAINING_PATH);
		
		csv.foreach(new VoidFunction<String>() {

			private static final long serialVersionUID = 1L;
			OHLCstock prevStock = null;
			OHLCstock currentStock = null;
			
			@Override
			public void call(String t) throws Exception {
				prevStock = currentStock;
				currentStock = new OHLCstock(t);
				
				if (prevStock != null) {
					JavaRDD<LabeledPoint> trainInstance = stocksToTrainRDD(prevStock, currentStock);
					
					System.out.println("prev:" + prevStock.getDate());
				}
				System.out.println("curr: " + currentStock.getDate());
				
			}
			
		});
				
	}

	public static JavaRDD<LabeledPoint> stocksToTrainRDD(OHLCstock s1, OHLCstock s2) {
		JavaRDD<LabeledPoint> input = null;
		return input;
	}

	public static JavaDStream<LabeledPoint> getDStreamTraining(JavaDStream<StockVariation> stocks) {

		return stocks.map(new Function<StockVariation, LabeledPoint>() {

			private static final long serialVersionUID = 1L;

			@Override
			public LabeledPoint call(StockVariation sv) throws Exception {
				// System.out.println("Inside LabeledPoint call : ----- ");

				Double label;
				Double pv = sv.getPriceVariation();
				Double vv = sv.getVolumeVariation();

				double vc[] = new double[numFeatures];
				vc[0] = pv;
				vc[1] = vv;

				if (pv > 0) {
					label = new Double(1);
				} else {
					label = new Double(0);
				}

				return new LabeledPoint(label, Vectors.dense(vc));
			}

		});
	}

	public static JavaDStream<Vector> getDStreamPrediction(JavaDStream<LabeledPoint> lines) {

		return lines.map(new Function<LabeledPoint, Vector>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Vector call(LabeledPoint sv) throws Exception {
				// Double label;
				// Double pv = sv.features();
				// Double vv = sv.getVolumeVariation();
				//
				// double vc[] = new double[numFeatures];
				// vc[0] = pv;
				// vc[1] = vv;
				return sv.features();
			}
		});
	}

}
