package it.himyd.cassandra;

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

	boolean resetData = false; // default
	String clusterIP = "127.0.0.1"; // default

	final String keyspaceName = "finance";
	final String ohlcTableName = "ohlc";
	final String clusterTableName = "clusters";

	// @formatter:off
    final String ohlcTable = "CREATE TABLE IF NOT EXISTS finance.ohlc("
	         + "symbol text, "
	         + "tradetime timestamp, "
	         + "open double, "
	         + "high double, "
	         + "low double, "
	         + "close double, "
	         + "volume int, "
	         + "PRIMARY KEY(symbol,tradetime) "
	         + ") WITH CLUSTERING ORDER BY (tradetime DESC);";
    
    final String clustersTable = "CREATE TABLE IF NOT EXISTS finance.clusters("
	         + "clustertime timestamp, "
	         + "cluster int, "
	         + "symbol text, "
	         + "PRIMARY KEY(symbol,clustertime) "
	         + ") WITH CLUSTERING ORDER BY (clustertime DESC);";
    // @formatter:on

	public static void main(String[] args) {
		new CassandraManager();
	}

	public CassandraManager() {
	}

	public void initialize() {
		cluster = Cluster.builder().addContactPoint(clusterIP).build();
		session = cluster.connect();

		if (resetData) {
			dropKeyspace();
			System.out.println("Keyspacace dropped!");
		}

		createKeyspace();

		createTable(ohlcTable);
		createTable(clustersTable);
	}

	private void dropKeyspace() {
		String query = "DROP KEYSPACE " + keyspaceName + ";";

		// Executing the query
		session.execute(query);

		System.out.println("Keyspace deleted");
	}

	private void createKeyspace() {
		String query = "CREATE KEYSPACE IF NOT EXISTS " + keyspaceName
				+ " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };";

		session.execute(query);
		session.execute("USE finance;");

		System.out.println("Keyspace created (or already present)");
	}

	private void createTable(String table) {

		session.execute(table);

		System.out.println("Table created (or already present)");
	}

	public void persistOHLCStocks(JavaDStream<StockOHLC> ohlc) {
		javaFunctions(ohlc).writerBuilder(keyspaceName, ohlcTableName, mapToRow(StockOHLC.class)).saveToCassandra();
	}

	public void persistClusterStocks(JavaDStream<StockCluster> clusters) {
		javaFunctions(clusters).writerBuilder(keyspaceName, clusterTableName, mapToRow(StockCluster.class))
				.saveToCassandra();
	}

	public JavaRDD<StockOHLC> readOHLCStocks(JavaSparkContext jsc) {
		SparkContextJavaFunctions spjf = com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions(jsc);
		RowReaderFactory<StockOHLC> rrf = com.datastax.spark.connector.japi.CassandraJavaUtil.mapRowTo(StockOHLC.class);
		JavaRDD<StockOHLC> rdd = spjf.cassandraTable(keyspaceName, ohlcTableName, rrf);

		return rdd;
	}

	public JavaRDD<StockCluster> readClusterStocks(JavaSparkContext jsc) {
		SparkContextJavaFunctions spjf = com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions(jsc);
		RowReaderFactory<StockCluster> rrf = com.datastax.spark.connector.japi.CassandraJavaUtil
				.mapRowTo(StockCluster.class);
		JavaRDD<StockCluster> rdd = spjf.cassandraTable(keyspaceName, clusterTableName, rrf);

		return rdd;
	}

	public Cluster getCluster() {
		return cluster;
	}

	public void setCluster(Cluster cluster) {
		this.cluster = cluster;
	}

	public Session getSession() {
		return session;
	}

	public void setSession(Session session) {
		this.session = session;
	}

	public String getClusterIP() {
		return clusterIP;
	}

	public void setClusterIP(String clusterIP) {
		this.clusterIP = clusterIP;
	}

	public boolean isResetData() {
		return resetData;
	}

	public void setResetData(boolean resetData) {
		this.resetData = resetData;
	}

}
