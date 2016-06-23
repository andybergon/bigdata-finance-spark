package it.himyd.persistence.cassandra;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;
import static com.datastax.spark.connector.japi.CassandraStreamingJavaUtil.javaFunctions;

import org.apache.spark.streaming.api.java.JavaDStream;

import it.himyd.stock.StockSample;

public class CassandraManager {

	public void persistStocks(JavaDStream<StockSample> sampleStocks) {
		javaFunctions(sampleStocks).writerBuilder("finance", "stocks", mapToRow(StockSample.class)).saveToCassandra();
	}

	// NOT WORKING

	// JavaRDD<StockSample> sampleStockRDD = sampleStocks.compute(new Time(60*1000));
	// javaFunctions(sampleStockRDD).writerBuilder("finance", "stocks",
	// mapToRow(StockSample.class)).saveToCassandra();

	// CassandraJavaUtil.mapToRow(StockSample.class)
	// RowWriterFactory<StockSample> rowWriterFactory
	// javaFunctions(sampleStocks).writerBuilder("finance", "stocks",
	// rowWriterFactory).saveToCassandra();

	// javaFunctions(sampleStockRDD, StockSample.class);

	// JavaRDD<String> cassandraRowsRDD = javaFunctions(jssc).cassandraTable("finance",
	// "stocks").map(new Function<CassandraRow, String>() {
	// @Override
	// public String call(CassandraRow cassandraRow) throws Exception {
	// return cassandraRow.toString();
	// }
	// });

	// StringUtils.join(cassandraRowsRDD.take(1), "\n")
	// System.out.println("Data as CassandraRows: \n" + cassandraRowsRDD.count());
}
