package it.himyd.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

import it.himyd.storm.bolts.StockCreatorBolt;
import it.himyd.storm.bolts.WordCounterBolt;
import it.himyd.storm.bolts.WordSpitterBolt;
import it.himyd.storm.spouts.KafkaReaderSpout;
import it.himyd.storm.spouts.LineReaderSpout;

public class HelloStorm {

	public static void main(String[] args) throws Exception {
		Config config = new Config();
		config.put("inputFile", args[0]);
		config.setDebug(true);
		config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("kafka-spout", new KafkaReaderSpout());
		builder.setBolt("stock-creator", new StockCreatorBolt()).shuffleGrouping("kafka-spout");
		// change grouping
		// builder.setBolt("stock-ohlc-creator", new WordCounterBolt()).shuffleGrouping("word-spitter");

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("HelloStorm", config, builder.createTopology());
		Thread.sleep(10000);

		cluster.shutdown();
	}

}