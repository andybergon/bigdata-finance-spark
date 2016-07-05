package it.himyd.spark.ml.classification;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;

import it.himyd.stock.StockOHLC;

public class StockClassifier {
	private final static String TRAINING_PATH = "target/classes/ml/BANKNIFTY.txt";

	private static SVMModel svmModel;

	private final static int NUM_FEATURES = 1;

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SparkStockAnalytics");
		conf.setMaster("local[2]");

		JavaSparkContext jsc = new JavaSparkContext(conf);

		trainOnIndianStock(jsc);

		double[] testData = new double[NUM_FEATURES];
		testData[0] = -0.07;

		Double predict = svmModel.predict(Vectors.dense(testData));

		if (predict.equals(0.0)) {
			System.out.println("Stock will go DOWN - " + predict);
		} else {
			System.out.println("Stock will go UP - " + predict);
		}

	}

	private static void trainOnIndianStock(JavaSparkContext jsc) {

		JavaRDD<String> csv = jsc.textFile(TRAINING_PATH);

		JavaRDD<LabeledPoint> labeled = csv.map(new Function<String, LabeledPoint>() {

			private static final long serialVersionUID = 1L;
			StockOHLC prevStock = null;
			StockOHLC currentStock = null;

			@Override
			public LabeledPoint call(String t) throws Exception {
				Double label;
				prevStock = currentStock;
				currentStock = new StockOHLC(t);

				if (prevStock != null) {
					// System.out.println("prev:" + prevStock.getDate());
					// System.out.println("curr: " + currentStock.getDate());

					Double prevPriceDiff = prevStock.getClose() / prevStock.getOpen();
					prevPriceDiff = (prevPriceDiff - 1) * 100;

					double vc[] = new double[NUM_FEATURES];
					vc[0] = prevPriceDiff;

					Double currPriceDiff = currentStock.getClose() / currentStock.getOpen();
					currPriceDiff = (currPriceDiff - 1) * 100;

					// for now: only up and down. TODO: remain or exact value
					// can't have 3 cases with this model
					if (currPriceDiff <= 0) {
						label = 0.0; // down
					} else {
						label = 1.0; // up
					}
					
					System.out.println(currentStock.getTradeTime().getTime() + " - prev: " + prevPriceDiff + ", curr: " + currPriceDiff);

					return new LabeledPoint(label, Vectors.dense(vc));
				}

				// TODO: caso del primo stock che non ho prev
				return new LabeledPoint(0, Vectors.dense(new double[] { 0 }));

			}

		});

		svmModel = SVMWithSGD.train(labeled.rdd(), 10);

	}

}
