package it.himyd.storm;

import java.util.Arrays;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.KafkaConfig;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.starter.bolt.RollingCountBolt;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.trident.spout.RichSpoutBatchExecutor;
import org.apache.storm.tuple.Fields;

import it.himyd.storm.bolts.StockCreatorBolt;
import it.himyd.storm.bolts.WordCounterBolt;
import it.himyd.storm.bolts.WordSpitterBolt;
import it.himyd.storm.spouts.KafkaReaderSpout;
import it.himyd.storm.spouts.LineReaderSpout;

public class HelloStormKafka {
/*
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
	*/
	/*
	public static void main(String[] args) {

        String zkIp = "localhost";

        String nimbusHost = "localhost";

        String zookeeperHost = zkIp +":2181";

        ZkHosts zkHosts = new ZkHosts(zookeeperHost);

        SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, "stock_topic", "", "stock");

//        kafkaConfig.scheme = new SchemeAsMultiScheme(new JsonScheme() {
//            @Override
//            public Fields getOutputFields() {
//                return new Fields("events");
//            }
//        });

        KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("kafka-reader-spout", kafkaSpout);

        builder.setBolt("stock-creator-bolt", new RollingCountBolt(20, 3));
                //.fieldsGrouping("requestsEmitter", new Fields("request"));

        //More bolts stuffzz

        Config config = new Config();

        config.setMaxTaskParallelism(5);
        config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 2);
        config.put(Config.NIMBUS_HOST, nimbusHost);
        config.put(Config.NIMBUS_THRIFT_PORT, 6627);
        config.put(Config.STORM_ZOOKEEPER_PORT, 2181);
        config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(zkIp));

        try {
            StormSubmitter.submitTopology("my-topology", config, builder.createTopology());
        } catch (Exception e) {
            throw new IllegalStateException("Couldn't initialize the topology", e);
        }
    }
	 */
    
/*    
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException  {

		
		Config stormConfig = new Config();
		stormConfig.put(RichSpoutBatchExecutor.MAX_BATCH_SIZE_CONF, 100);
		stormConfig.put("nimbus.host", "localhost");
		// stormConfig.put("svend.example.cassandra.ip", CASSANDRA_IP);          // read in the CassandraDB

        TridentKafkaConfig kafkaConf = new TridentKafkaConfig(
                KafkaConfig.StaticHosts.fromHostString(Collections.singletonList("localhost"), 1) ,
                "room_events" );
        kafkaConf.forceFromStart = true;
        kafkaConf.fetchSizeBytes = 10000;

        StormSubmitter.submitTopology("occupancyTopology", stormConfig, BuildTopology.build(kafkaConf));
	}
	*/
}