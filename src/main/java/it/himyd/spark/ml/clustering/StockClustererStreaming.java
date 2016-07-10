package it.himyd.spark.ml.clustering;

import java.io.Serializable;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.clustering.StreamingKMeans;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import it.himyd.stock.StockCluster;
import it.himyd.stock.StockOHLC;
import it.himyd.stock.finance.yahoo.Stock;
import scala.Tuple2;

public class StockClustererStreaming implements Serializable {
	private static final long serialVersionUID = 1L;

	public static final int CLUSTERING_FEATURE_NUMBER = 4; // N.B. change to other clustering

	private StreamingKMeans model;
	private int clusterNumber;
	private Double scalingFactor;

	public StockClustererStreaming() {
		this.clusterNumber = 3;
		this.scalingFactor = new Double(1);

		this.model = new StreamingKMeans();
		this.model.setK(clusterNumber);
		// with a=1 all data will be used from the beginning
		// with a=0 only the most recent data will be used
		this.model.setDecayFactor(1.0);
		this.model.setRandomCenters(CLUSTERING_FEATURE_NUMBER, 0.0, 0L);
	}

	public JavaDStream<StockCluster> clusterOHLC(JavaDStream<StockOHLC> stocks) {
		JavaDStream<StockCluster> cluster = clusterStocksOHLC(stocks)
				.map(new Function<Tuple2<String, Integer>, StockCluster>() {

					private static final long serialVersionUID = 1L;

					@Override
					public StockCluster call(Tuple2<String, Integer> v1) throws Exception {
						StockCluster st = new StockCluster();
						st.setSymbol(v1._1());
						st.setCluster(v1._2());
						// st.setClustertime(Calendar.getInstance());

						return st;
					}
				});

		return cluster;
	}

	public JavaPairDStream<String, Integer> clusterStocks(JavaDStream<Stock> stocks) {
		trainModelOnStock(stocks); // train
		return convertLabeledVectorToCluster(convertStockToLabeledVector(stocks)); // predict
	}

	public JavaPairDStream<String, Integer> clusterStocksOHLC(JavaDStream<StockOHLC> stocks) {
		trainModelOnOHLCStock(stocks); // train
		return convertLabeledVectorToCluster(convertOHLCstockToLabeledVector(stocks)); // predict
	}

	public void trainModelOnStock(JavaDStream<Stock> trainData) {
		this.model.trainOn(convertStockToVector(trainData));
	}

	public void trainModelOnOHLCStock(JavaDStream<StockOHLC> trainData) {
		this.model.trainOn(convertOHLCstockToVector(trainData));
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

	public JavaDStream<Vector> convertOHLCstockToVector(JavaDStream<StockOHLC> stocks) {
		JavaDStream<Vector> trainingData = stocks.map(new Function<StockOHLC, Vector>() {
			private static final long serialVersionUID = 1L;

			public Vector call(StockOHLC s) {
				Vector vector = stockOHLCToVector(s);
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

	public JavaDStream<Tuple2<String, Vector>> convertOHLCstockToLabeledVector(JavaDStream<StockOHLC> stocks) {
		JavaDStream<Tuple2<String, Vector>> testData = stocks.map(new Function<StockOHLC, Tuple2<String, Vector>>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<String, Vector> call(StockOHLC s) throws Exception {
				Vector vector = stockOHLCToVector(s);
				return new Tuple2<String, Vector>(s.getSymbol(), vector);
			}

		});

		return testData;
	}

	public JavaPairDStream<String, Integer> convertLabeledVectorToCluster(
			JavaDStream<Tuple2<String, Vector>> testData) {

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
		values[3] = s.getQuote().getDayLow().doubleValue();

		return Vectors.dense(values);
	}

	// features of clustering
	public Vector stockOHLCToVector(StockOHLC stock) {
		Double percOL = stock.getOpen() / stock.getLow();
		percOL = (percOL - 1) * 100 * scalingFactor;
		Double percOH = stock.getOpen() / stock.getHigh();
		percOH = (percOH - 1) * 100 * scalingFactor;
		Double percHL = stock.getHigh() / stock.getLow();
		percHL = (percHL - 1) * 100 * scalingFactor;
		Double percCO = stock.getClose() / stock.getOpen();
		percCO = (percCO - 1) * 100;

		double[] values = new double[CLUSTERING_FEATURE_NUMBER];
		values[0] = percOL;
		values[1] = percOH;
		values[2] = percHL;
		values[3] = percCO;

		// System.out.println(Vectors.dense(values));
		return Vectors.dense(values);
	}

	public StreamingKMeans getModel() {
		return model;
	}

	public void setModel(StreamingKMeans model) {
		this.model = model;
	}

	public int getClusterNumber() {
		return clusterNumber;
	}

	public void setClusterNumber(int clusterNumber) {
		this.clusterNumber = clusterNumber;
	}

	public Double getScalingFactor() {
		return scalingFactor;
	}

	public void setScalingFactor(Double scalingFactor) {
		this.scalingFactor = scalingFactor;
	}

}
