package it.himyd.spark.kmeans;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.StreamingKMeans;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import it.himyd.stock.StockSample;

public class KMeansStreaming {
	public static final int CLUSTER_NUMBER = 2;

	JavaStreamingContext jssc;
	JavaPairInputDStream<String, String> stream;

	public KMeansStreaming() {}

	public KMeansStreaming(JavaStreamingContext jssc) {
		this.jssc = jssc;
	}

	public KMeansStreaming(JavaPairInputDStream<String, String> stream) {
		this.stream = stream;
	}

	public void clusterStocks(JavaDStream<StockSample> sampleStocks) {
		// sampleStocks.map(Vectors.parse(null));

		JavaDStream<Vector> trainingData = sampleStocks.map(new Function<StockSample, Vector>() {
			private static final long serialVersionUID = 1L;

			public Vector call(StockSample s) {
				double[] values = new double[1];
				values[0] = s.getPrice();
				return Vectors.dense(values);
			}

		});

		trainingData.cache();
		System.out.println("train " + trainingData.count());

		StreamingKMeans model = new StreamingKMeans();
		model.setK(CLUSTER_NUMBER);
		// with a=1 all data will be used from the beginning
		// with a=0 only the most recent data will be used
		model.setDecayFactor(1.0); 
		model.setRandomCenters(2, 0.0,0L); // che valori passare?
		model.trainOn(trainingData);
		
		
//		model.predictOnValues(testData.mapToPair(new PairFunction<LabeledPoint, Double, Vector>() {
//				public Tuple2<Double, Vector> call(LabeledPoint arg0)
//						throws Exception {
//					return new Tuple2<Double, Vector>(arg0.label(), arg0.features());
//				}
//			})).print();

	}

}
