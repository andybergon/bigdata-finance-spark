package it.himyd.analysis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import it.himyd.finance.yahoo.Stock;
import it.himyd.stock.StockSample;
import scala.Tuple2;

public class AnalysisRunner {
	
	public JavaDStream<Stock> convertKafkaMessagesToStock(JavaPairInputDStream<String, String> messages) {
		JavaDStream<Stock> stocks = messages.map(line -> Stock.fromJSONString((line._2)));
		return stocks;
	}
	
	public JavaDStream<StockSample> convertStockToStockSample(JavaDStream<Stock> stocks) {
		JavaDStream<StockSample> stockSample = stocks.map(stock -> new StockSample(stock));
		return stockSample;
	}
	
	// utilized before when kafka put all data in 1 line
	public JavaDStream<Stock> convertKafkaMessagesToStockOLD(JavaPairInputDStream<String, String> messages) {
		
		ObjectMapper mapper = new ObjectMapper();
		JavaDStream<Stock> messagesSplitted = messages.flatMap(new FlatMapFunction<Tuple2<String, String>, Stock>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Stock> call(Tuple2<String, String> t) throws Exception {
				Map<String, String> map = mapper.readValue(t._2(), new TypeReference<Map<String, Object>>() {});
				List<Stock> stocksList = new ArrayList<Stock>();
				
				for (String stockString : map.values()) {
					stocksList.add(Stock.fromJSONString(stockString));
				}

				return stocksList;
			}
			
		});
		
		return messagesSplitted;
	}
	
}
