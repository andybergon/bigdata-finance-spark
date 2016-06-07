package it.himyd.spark;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import org.apache.spark.streaming.kafka.KafkaUtils;

import it.himyd.finance.yahoo.Stock;
import it.himyd.stock.StockPrice;
import kafka.serializer.StringDecoder;

import scala.Tuple2;

public class SparkStockAnalyticsSingle {

	public static void main(String s[]) throws Exception {
		
		// Create a local StreamingContext with two working thread and batch interval of 1 second
		SparkConf conf = new SparkConf().setAppName("SparkStockAnalytics");
		// to run in eclipse, uncomment the following line
		conf.setMaster("local[2]");		
		

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        
        String brokers = "localhost:9092";
        String topics = "stock_topic";
        
        Map<String, String> kafkaParams = new HashMap<String, String>();
        Set<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
        
        kafkaParams.put("metadata.broker.list", brokers);
        System.out.println("Parameters Setted");
        
        // params: 
        // streamingContext,
        // [key class], [value class], 
        // [key decoder class], [value decoder class],
        // [map of Kafka parameters], [set of topics to consume]
        
//        JavaReceiverInputDStream<String> messages = KafkaUtils.createDirectStream(
//                jssc,
//                String.class,
//                String.class,
//                StringDecoder.class,
//                StringDecoder.class,
//                kafkaParams,
//                topicsSet
//        );
        
        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topicsSet
        );    
        
         messages.print();
        // messages.foreach(System.out.println(line));
        
		JavaDStream<Stock> stockPrices = messages.map(line -> Stock.fromJSONString((line._2)));
        System.out.println(stockPrices.toString());
        
        JavaPairDStream<String,Double> avg = stockPrices
        		.mapToPair(new AverageMap())
        		.reduceByKeyAndWindow(new AverageReduce(), Durations.seconds(10), Durations.seconds(2))
        		.mapToPair(new AverageMap2());
        
        avg.print();
		
        jssc.start();
        jssc.awaitTermination();
		
	}
	
	private static final class AverageMap implements PairFunction<Stock, String, Tuple2<Double,Integer>> {
		private static final long serialVersionUID = 1L;
		
		@Override
		public Tuple2<String,Tuple2<Double, Integer>> call(Stock stock) throws Exception {
			String symbol = stock.getSymbol();
			Double price = stock.getQuote().getPrice().doubleValue();
			
			return new Tuple2<>(symbol,new Tuple2<>(price, new Integer(1)));
		}
		
	}
	
	private static final class AverageReduce implements Function2<Tuple2<Double,Integer>, Tuple2<Double,Integer>, Tuple2<Double,Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<Double,Integer> call(Tuple2<Double,Integer> i1, Tuple2<Double,Integer> i2) {
			Tuple2<Double,Integer> tuple = new Tuple2<Double,Integer>(i1._1 + i2._1, i1._2 + i2._2);
			
			return tuple;
		}
	}
	
	private static final class AverageMap2 implements PairFunction<Tuple2<String,Tuple2<Double,Integer>>, String, Double> {
		private static final long serialVersionUID = 1L;
		
		@Override
		public Tuple2<String,Double> call(Tuple2<String,Tuple2<Double,Integer>> tuple) throws Exception {
			String symbol = tuple._1;
			Double avg = tuple._2._1/tuple._2._2;
			
			return new Tuple2<>(symbol,avg);
		}
		
	}
	
	private static final class StockToPair implements PairFunction<StockPrice, String, Double> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<String, Double> call(StockPrice stock) throws Exception {
			String symbol = stock.getSymbol();
//			Double price = stock.getQuote().getPrice().doubleValue();
			Double price = stock.getPrice();

			return new Tuple2<>(symbol, price);
		}

	}
	
	
	private static final class Sum implements Function2<Double, Double, Double> {
		private static final long serialVersionUID = 1L;

		@Override
		public Double call(Double i1, Double i2) {
			return i1 + i2;
		}
	}
}
