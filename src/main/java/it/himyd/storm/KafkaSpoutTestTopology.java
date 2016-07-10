package it.himyd.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class KafkaSpoutTestTopology {
	public static final Logger LOG = LoggerFactory.getLogger(KafkaSpoutTestTopology.class);

	public static class PrinterBolt extends BaseBasicBolt {
		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
		}

		@Override
		public void execute(Tuple tuple, BasicOutputCollector collector) {
			LOG.info(tuple.toString());
		}

	}

	private final BrokerHosts brokerHosts;

	public KafkaSpoutTestTopology(String kafkaZookeeper) {
		brokerHosts = new ZkHosts(kafkaZookeeper);
	}

	public StormTopology buildTopology() {
		SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, "stock_topic", "", "storm");
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("stocks", new KafkaSpout(kafkaConfig), 10);
		builder.setBolt("print", new PrinterBolt());//.shuffleGrouping("stocks");
		return builder.createTopology();
	}

	public static void main(String[] args) throws Exception {

//		String kafkaZk = args[0];
		String kafkaZk = "localhost";

		KafkaSpoutTestTopology kafkaSpoutTestTopology = new KafkaSpoutTestTopology(kafkaZk);
		Config config = new Config();
		config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 2000);

		StormTopology stormTopology = kafkaSpoutTestTopology.buildTopology();
		
		if (args != null && args.length > 1) {
			String name = args[1];
			String dockerIp = args[2];
			config.setNumWorkers(2);
			config.setMaxTaskParallelism(5);
			config.put(Config.NIMBUS_HOST, dockerIp);
			config.put(Config.NIMBUS_THRIFT_PORT, 6627);
			config.put(Config.STORM_ZOOKEEPER_PORT, 2181);
			config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(dockerIp));
			StormSubmitter.submitTopology(name, config, stormTopology);
		} else {
			config.setNumWorkers(2);
			config.setMaxTaskParallelism(2);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("kafka", config, stormTopology);
		}
	}
}