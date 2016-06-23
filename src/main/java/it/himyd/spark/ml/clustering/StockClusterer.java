package it.himyd.spark.ml.clustering;

import java.io.Serializable;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.clustering.StreamingKMeans;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import it.himyd.finance.yahoo.Stock;
import scala.Tuple2;

public class StockClusterer implements Serializable {
	private static final long serialVersionUID = 1L;

	public static final int CLUSTER_NUMBER = 2;
	public final static int CLUSTERING_FEATURE_NUMBER = 3;

	StreamingKMeans model = new StreamingKMeans();

	public StockClusterer() {
		this.model = new StreamingKMeans();

		this.model.setK(CLUSTER_NUMBER);
		// with a=1 all data will be used from the beginning
		// with a=0 only the most recent data will be used
		this.model.setDecayFactor(1.0);
		this.model.setRandomCenters(CLUSTERING_FEATURE_NUMBER, 0.0, 0L);
	}
	
	public JavaPairDStream<String, Integer> clusterStocks(JavaDStream<Stock> stocks){
		trainModelOnStock(stocks); // train
		return convertLabeledVectorToCluster(convertStockToLabeledVector(stocks)); // predict
	}

	
	public JavaDStream<Vector> convertStockToVector(JavaDStream<Stock> stocks) {
		JavaDStream<Vector> trainingData = stocks.map(new Function<Stock, Vector>() {
			private static final long serialVersionUID = 1L;

			public Vector call(Stock s) {
				Vector vector = stockToVector(s);
				return vector;
			}

		});

		trainingData.cache(); // check

		return trainingData;
	}

	public JavaDStream<Tuple2<String, Vector>> convertStockToLabeledVector(JavaDStream<Stock> stocks) {
		JavaDStream<Tuple2<String, Vector>> testData = stocks.map(new Function<Stock, Tuple2<String, Vector>>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<String, Vector> call(Stock s) throws Exception {
				Vector vector = stockToVector(s);
				return new Tuple2<String, Vector>(s.getSymbol(), vector);
			}

		});

		return testData;
	}

	public void trainModelOnStock(JavaDStream<Stock> trainData) {
		this.model.trainOn(convertStockToVector(trainData));
	}

	public JavaPairDStream<String, Integer> convertLabeledVectorToCluster(JavaDStream<Tuple2<String, Vector>> testData) {
		
		JavaPairDStream<String, Integer> predictedData = this.model
				.predictOnValues(testData.mapToPair(new PairFunction<Tuple2<String, Vector>, String, Vector>() {
					private static final long serialVersionUID = 1L;
					
					@Override
					public Tuple2<String, Vector> call(Tuple2<String, Vector> t) throws Exception {
						return new Tuple2<String, Vector>(t._1, t._2());
					}
					
				}));
		
		return predictedData;
	}
	
	// change here to change features of clustering
	public Vector stockToVector(Stock s) {
		double[] values = new double[CLUSTERING_FEATURE_NUMBER];
		values[0] = s.getQuote().getPrice().doubleValue();
		values[1] = (double) s.getQuote().getVolume();
		values[2] = s.getQuote().getDayHigh().doubleValue();
		// values[3] = s.getQuote().getDayLow().doubleValue();
		return Vectors.dense(values);
	}

}
