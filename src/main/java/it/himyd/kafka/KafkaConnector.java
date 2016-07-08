package it.himyd.kafka;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;

public class KafkaConnector {
	private JavaStreamingContext jssc;
	private Map<String, String> kafkaParams;
	private Set<String> topicsSet;

	private String brokerAddress;
	private String topics;

	public KafkaConnector(JavaStreamingContext jssc) {
		this.jssc = jssc;

		System.out.println("Address NOT defined, using 'localhost' as default.");
		this.brokerAddress = "localhost:9092";
		this.topics = "stock_topic";

		this.initialize();
	}

	public KafkaConnector(JavaStreamingContext jssc, String address) {
		this.jssc = jssc;

		this.brokerAddress = address + ":9092";
		this.topics = "stock_topic";

		this.initialize();
	}

	public KafkaConnector(JavaStreamingContext jssc, Map<String, String> kafkaParams, Set<String> topicsSet) {
		this.jssc = jssc;
		this.kafkaParams = kafkaParams;
		this.topicsSet = topicsSet;
	}

	public void initialize() {
		System.out.println("Setting Kafka Parameters...");

		this.kafkaParams = new HashMap<String, String>();
		this.kafkaParams.put("metadata.broker.list", brokerAddress);

		this.topicsSet = new HashSet<String>();
		this.topicsSet.addAll(Arrays.asList(topics.split(",")));

		System.out.println("Kafka Parameters Setted");
	}

	// params:
	// streamingContext,
	// [key class],
	// [value class],
	// [key decoder class],
	// [value decoder class],
	// [map of Kafka parameters],
	// [set of topics to consume]
	public JavaPairInputDStream<String, String> getStream() {
		JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(this.jssc, String.class,
				String.class, StringDecoder.class, StringDecoder.class, this.kafkaParams, this.topicsSet);

		return messages;
	}

	public JavaStreamingContext getJssc() {
		return jssc;
	}

	public void setJssc(JavaStreamingContext jssc) {
		this.jssc = jssc;
	}

	public Map<String, String> getKafkaParams() {
		return kafkaParams;
	}

	public void setKafkaParams(Map<String, String> kafkaParams) {
		this.kafkaParams = kafkaParams;
	}

	public Set<String> getTopicsSet() {
		return topicsSet;
	}

	public void setTopicsSet(Set<String> topicsSet) {
		this.topicsSet = topicsSet;
	}

	public String getBrokerAddress() {
		return brokerAddress;
	}

	public void setBrokerAddress(String brokerAddress) {
		this.brokerAddress = brokerAddress;
	}

	public String getTopics() {
		return topics;
	}

	public void setTopics(String topics) {
		this.topics = topics;
	}

}
