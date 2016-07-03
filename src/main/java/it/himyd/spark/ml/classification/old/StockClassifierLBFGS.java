package it.himyd.spark.ml.classification.old;
//package it.himyd.spark.ml.classification;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.mllib.classification.StreamingLogisticRegressionWithSGD;
//import org.apache.spark.mllib.linalg.Vector;
//import org.apache.spark.mllib.linalg.Vectors;
//import org.apache.spark.mllib.optimization.LBFGS;
//import org.apache.spark.mllib.optimization.LogisticGradient;
//import org.apache.spark.mllib.optimization.SquaredL2Updater;
//import org.apache.spark.mllib.regression.LabeledPoint;
//import org.apache.spark.streaming.Duration;
//import org.apache.spark.streaming.Durations;
//import org.apache.spark.streaming.api.java.JavaDStream;
//import org.apache.spark.streaming.api.java.JavaPairDStream;
//import org.apache.spark.streaming.api.java.JavaPairInputDStream;
//import org.apache.spark.streaming.api.java.JavaStreamingContext;
//
//import it.himyd.analysis.AnalysisRunner;
//import it.himyd.finance.yahoo.Stock;
//import it.himyd.kafka.KafkaConnector;
//import it.himyd.stock.StockVariation;
//import scala.Tuple2;
//
//public class StockClassifierLBFGS {
//	private final static Duration BATCH_DURATION = Durations.seconds(1);
//	
//	private static StreamingLogisticRegressionWithSGD slrModel;
//
//	private static int numFeatures = 2;
//
//	public static void main(String[] args) {
//		SparkConf conf = new SparkConf().setAppName("SparkStockAnalytics");
//		conf.setMaster("local[2]");
//
//		JavaStreamingContext jssc = new JavaStreamingContext(conf, BATCH_DURATION);
//
//		KafkaConnector kc = new KafkaConnector(jssc);
//		JavaPairInputDStream<String, String> messages = kc.getStream();
//		messages.print();
//
//		AnalysisRunner ar = new AnalysisRunner();
//		JavaDStream<Stock> stocks = ar.convertKafkaMessagesToStock(messages);
//		JavaDStream<StockVariation> variation = ar.percentageVariation(stocks);
//		variation.print();
//
//		slrModel = new StreamingLogisticRegressionWithSGD();
//		slrModel.setStepSize(0.5);
//		slrModel.setNumIterations(10);
//		slrModel.setInitialWeights(Vectors.zeros(numFeatures));
//
//		JavaDStream<LabeledPoint> trainingData = getDStreamTraining(variation);
//		trainingData.cache();
//		
//		// Run training algorithm to build the model.
//		int numCorrections = 10;
//		double convergenceTol = 1e-4;
//		int maxNumIterations = 20;
//		double regParam = 0.1;
//		Vector initialWeightsWithIntercept = Vectors.dense(new double[numFeatures + 1]);
//
//		Tuple2<Vector, double[]> result = LBFGS.runLBFGS(
//		  trainingData.rdd(),
//		  new LogisticGradient(),
//		  new SquaredL2Updater(),
//		  numCorrections,
//		  convergenceTol,
//		  maxNumIterations,
//		  regParam,
//		  initialWeightsWithIntercept);
//		Vector weightsWithIntercept = result._1();
//		double[] loss = result._2();
//		
//		
//		
//
//		jssc.start();
//		jssc.awaitTermination();
//	}
//
//	public static JavaDStream<LabeledPoint> getDStreamTraining(JavaDStream<StockVariation> stocks) {
//
//		return stocks.map(new Function<StockVariation, LabeledPoint>() {
//
//			private static final long serialVersionUID = 1268686043314386060L;
//
//			@Override
//			public LabeledPoint call(StockVariation sv) throws Exception {
//				System.out.println("Inside LabeledPoint call : ----- ");
//
//				Double label;
//				Double pv = sv.getPriceVariation();
//				Double vv = sv.getVolumeVariation();
//
//				double vc[] = new double[numFeatures];
//				vc[0] = pv;
//				vc[1] = vv;
//
//				if (pv > 0) {
//					label = new Double(1);
//				} else if (pv < 0) {
//					label = new Double(2);
//				} else {
//					label = new Double(0);
//				}
//
//				return new LabeledPoint(label, Vectors.dense(vc));
//			}
//
//		});
//	}
//
//	// public static JavaDStream<Vector>
//	// getDStreamPrediction(JavaStreamingContext context) {
//	// JavaReceiverInputDStream<String> lines =
//	// context.socketTextStream("localhost", 9999);
//	//
//	// return lines.map(new Function<String, Vector>() {
//	//
//	// private static final long serialVersionUID = 1268686043314386060L;
//	//
//	// @Override
//	// public Vector call(String data) throws Exception {
//	// System.out.println("Inside Vector call : ----- ");
//	// String vcS[] = data.split("-");
//	// double vc[] = new double[3];
//	// int i = 0;
//	// for (String vcSi : vcS) {
//	// vc[i++] = Double.parseDouble(vcSi);
//	// }
//	// return Vectors.dense(vc);
//	// }
//	// });
//	// }
//}
