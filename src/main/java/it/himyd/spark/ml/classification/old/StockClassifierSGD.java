package it.himyd.spark.ml.classification.old;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.StreamingLogisticRegressionWithSGD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
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
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;

import it.himyd.kafka.KafkaConnector;
import it.himyd.spark.analysis.streaming.AnalysisRunner;
import it.himyd.stock.StockVariation;
import it.himyd.stock.finance.yahoo.Stock;

public class StockClassifierSGD {
	private final static Duration BATCH_DURATION = Durations.seconds(5);

	private static StreamingLogisticRegressionWithSGD slrModel;

	private static int numFeatures = 2;

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SparkStockAnalytics");
		conf.setMaster("local[2]");

		JavaStreamingContext jssc = new JavaStreamingContext(conf, BATCH_DURATION);

		// RIPORTA
		jssc.checkpoint(".");
		
		KafkaConnector kc = new KafkaConnector(jssc);
		JavaPairInputDStream<String, String> messages = kc.getStream();
//		messages.print();

		AnalysisRunner ar = new AnalysisRunner();
		JavaDStream<Stock> stocks = ar.convertKafkaMessagesToStock(messages);
		// JavaDStream<StockVariation> variation =
		// ar.percentageVariation(stocks);
		// variation.print();

		JavaPairDStream<String, StockVariation> variationPair = ar.percentageVariationPair(stocks);
//		variationPair.print();

		List<Tuple2<String,StockVariation>> tupleList = new ArrayList<>();
		JavaPairRDD<String, StockVariation> initialRDD = jssc.sparkContext().parallelizePairs(tupleList);
		// JavaPairDStream<String, Integer> wordsDstream = null;

		Function4<Time,String,Optional<StockVariation>, State<StockVariation>, Optional<StockVariation>> mf = new Function4<Time,String, Optional<StockVariation>, State<StockVariation>, Optional<StockVariation>>() {

			@Override
			public Optional<StockVariation> call(Time v1, String v2, Optional<StockVariation> v3, State<StockVariation> v4)
					throws Exception {
//				System.out.println("+++ " + v3.get().toString());
				return v3;
			}
		};
		
		final Function4<Time, String, Optional<StockVariation>, State<StockVariation>,Optional<StockVariation>> mappingFunc = new Function4<Time, String, Optional<StockVariation>, State<StockVariation>,Optional<StockVariation>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Optional<StockVariation> call(Time time, String label, Optional<StockVariation> one, State<StockVariation> state) {
				// Use all State's methods here
//				state.exists();
//				state.get();
//				state.isTimingOut();
//				state.remove();
//				state.update(one.get());
				return one;
			}

		};
		
		final Function3<String, Optional<StockVariation>, State<Double>, Tuple2<String,Double>> mappingFunc1 = new Function3<String, Optional<StockVariation>, State<Double>, Tuple2<String,Double>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Double> call(String label, Optional<StockVariation> one, State<Double> state) {
				// Use all State's methods here
//				state.exists();
//				state.get();
//				state.isTimingOut();
//				state.remove();
//				state.update(one.get());
				
				double sum = 100;
				if (state.exists()) {
//					System.out.println(state.get());
					sum += state.get();
				}
				
				Tuple2<String, Double> output = new Tuple2<>(one.get().getName(), sum);
				state.update(sum);
				return output;
			}

		};

		StateSpec stateSpec = StateSpec.function(mappingFunc1);
		stateSpec.initialState(initialRDD);
		stateSpec.numPartitions(10);
		stateSpec.partitioner(new HashPartitioner(10));
		stateSpec.timeout(Durations.seconds(10));


		testOperations(variationPair,stateSpec);


//		slrModel = new StreamingLogisticRegressionWithSGD();
//		slrModel.setStepSize(0.5);
//		slrModel.setNumIterations(10);
//		slrModel.setInitialWeights(Vectors.zeros(numFeatures));
//
//		JavaDStream<LabeledPoint> trainingData = getDStreamTraining(variation);
//		trainingData.cache();
//		slrModel.trainOn(trainingData);
//
//		JavaDStream<LabeledPoint> testData = getDStreamTraining(variation);
//
//		slrModel.predictOn(getDStreamPrediction(testData)).print();

		jssc.start();
		jssc.awaitTermination();
	}
	
	private static void testOperations(JavaPairDStream<String, StockVariation> variationPair, StateSpec stateSpec) {
		
		JavaMapWithStateDStream<String, StockVariation, StockVariation, Double> stateDstream = variationPair
				.mapWithState(stateSpec);
//		stateDstream.print();

		JavaPairDStream<String, StockVariation> stateSnapshots = stateDstream.stateSnapshots();
		stateSnapshots.print();
		
    final List<Set<Tuple2<String, StockVariation>>> collectedStateSnapshots =
            Collections.synchronizedList(Lists.<Set<Tuple2<String, StockVariation>>>newArrayList());
    		stateDstream.stateSnapshots().foreachRDD(new Function<JavaPairRDD<String, StockVariation>, Void>() {
          @Override
          public Void call(JavaPairRDD<String, StockVariation> rdd) throws Exception {
            collectedStateSnapshots.add(Sets.newHashSet(rdd.collect()));
            return null;
          }
        });
    		
    System.out.println(collectedStateSnapshots.toString());
	}

	public static JavaDStream<LabeledPoint> getDStreamTraining(JavaDStream<StockVariation> stocks) {

		return stocks.map(new Function<StockVariation, LabeledPoint>() {

			private static final long serialVersionUID = 1268686043314386060L;

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

			private static final long serialVersionUID = 1268686043314386060L;

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
