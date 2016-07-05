package it.himyd.persistence.cassandra;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;
import static com.datastax.spark.connector.japi.CassandraStreamingJavaUtil.javaFunctions;

import java.util.Date;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.functions;
import org.apache.spark.streaming.api.java.JavaDStream;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.CassandraRow;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;
import com.datastax.spark.connector.japi.rdd.CassandraJavaPairRDD;
import com.datastax.spark.connector.rdd.reader.RowReaderFactory;

import it.himyd.stock.StockCluster;
import it.himyd.stock.StockOHLC;
import it.himyd.stock.StockSample;

public class CassandraManager {
	Cluster cluster;
	Session session;

	String keyspace = "finance";

	// @formatter:off
    String ohlcTable = "CREATE TABLE IF NOT EXISTS finance.ohlc("
	         + "symbol text, "
	         + "tradetime timestamp, "
	         + "open double, "
	         + "high double, "
	         + "low double, "
	         + "close double, "
	         + "volume int, "
	         + "PRIMARY KEY(symbol,tradetime) "
	         + ") WITH CLUSTERING ORDER BY (tradetime DESC);";
    
    String clustersTable = "CREATE TABLE IF NOT EXISTS finance.clusters("
	         + "clustertime timestamp, "
	         + "cluster int, "
	         + "symbol text, "
	         + "PRIMARY KEY(symbol, clustertime) "
	         + ") WITH CLUSTERING ORDER BY (clustertime DESC);";
    // @formatter:on

	public static void main(String[] args) {
		new CassandraManager();
	}

	public CassandraManager() {
		// Creating Cluster object
		cluster = Cluster.builder().addContactPoint("127.0.0.1").build();

		// Creating Session object
		session = cluster.connect();

		// dropKeyspace();
		createKeyspace();
		createTable(clustersTable);
	}

	private void dropKeyspace() {
		String query = "DROP KEYSPACE finance;";

		// Executing the query
		session.execute(query);

		System.out.println("Keyspace deleted");
	}

	private void createKeyspace() {
		String query = "CREATE KEYSPACE IF NOT EXISTS finance WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };";

		// Executing the query
		session.execute(query);
		session.execute("USE finance;");

		System.out.println("Keyspace created");
	}

	private void createTable(String table) {
		// Executing the query
		session.execute(table);

		System.out.println("Table created");
	}

	public void persistStocks(JavaDStream<StockSample> sampleStocks) {
		javaFunctions(sampleStocks).writerBuilder(keyspace, "stocks", mapToRow(StockSample.class)).saveToCassandra();
	}

	public void persistOHLCStocks(JavaDStream<StockOHLC> ohlc) {
		javaFunctions(ohlc).writerBuilder(keyspace, "ohlc", mapToRow(StockOHLC.class)).saveToCassandra();
	}

	public void persistClusterStocks(JavaDStream<StockCluster> clusters) {
		javaFunctions(clusters).writerBuilder(keyspace, "clusters", mapToRow(StockCluster.class)).saveToCassandra();
	}

	public JavaRDD<StockCluster> readClusterStocks(SparkContext sc) {
		SparkContextJavaFunctions spjf = com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions(sc);
		RowReaderFactory<StockCluster> rrf = com.datastax.spark.connector.japi.CassandraJavaUtil
				.mapRowTo(StockCluster.class);
		JavaRDD<StockCluster> rdd = spjf.cassandraTable(keyspace, "clusters", rrf);

		return rdd;
	}
	
//	public JavaPairRDD<Date,StockCluster> readClusterStocksPair(SparkContext sc) {
//		SparkContextJavaFunctions spjf = com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions(sc);
//		RowReaderFactory<Date> rrfCol = com.datastax.spark.connector.japi.CassandraJavaUtil
//				.mapColumnTo(Date.class);
//		RowReaderFactory<StockCluster> rrfRow = com.datastax.spark.connector.japi.CassandraJavaUtil
//				.mapRowTo(StockCluster.class);
//		CassandraJavaPairRDD<Date, StockCluster> rdd = spjf.cassandraTable();
//
//		return rdd;
//	}

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
