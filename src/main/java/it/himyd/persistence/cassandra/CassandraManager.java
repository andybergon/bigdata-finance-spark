package it.himyd.persistence.cassandra;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;
import static com.datastax.spark.connector.japi.CassandraStreamingJavaUtil.javaFunctions;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaDStream;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;
import com.datastax.spark.connector.rdd.reader.RowReaderFactory;

import it.himyd.stock.StockCluster;
import it.himyd.stock.StockOHLC;

public class CassandraManager {
	Cluster cluster;
	Session session;
	boolean mustDropKeyspace = false;

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

	public CassandraManager(boolean mustDropKeyspace) {
		this.mustDropKeyspace = mustDropKeyspace;

		// Creating Cluster object
		cluster = Cluster.builder().addContactPoint("127.0.0.1").build();

		// Creating Session object
		session = cluster.connect();

		if (mustDropKeyspace) {
			dropKeyspace();
		}
		createKeyspace();
		createTable(clustersTable);
	}

	public CassandraManager() {
		// Creating Cluster object
		cluster = Cluster.builder().addContactPoint("127.0.0.1").build();

		// Creating Session object
		session = cluster.connect();

		if (mustDropKeyspace) {
			dropKeyspace();
		}
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

	public void persistOHLCStocks(JavaDStream<StockOHLC> ohlc) {
		javaFunctions(ohlc).writerBuilder(keyspace, "ohlc", mapToRow(StockOHLC.class)).saveToCassandra();
	}

	public void persistClusterStocks(JavaDStream<StockCluster> clusters) {
		javaFunctions(clusters).writerBuilder(keyspace, "clusters", mapToRow(StockCluster.class)).saveToCassandra();
	}

	public JavaRDD<StockCluster> readClusterStocks(JavaSparkContext jsc) {
		SparkContextJavaFunctions spjf = com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions(jsc);
		RowReaderFactory<StockCluster> rrf = com.datastax.spark.connector.japi.CassandraJavaUtil
				.mapRowTo(StockCluster.class);
		JavaRDD<StockCluster> rdd = spjf.cassandraTable(keyspace, "clusters", rrf);

		return rdd;
	}

}
