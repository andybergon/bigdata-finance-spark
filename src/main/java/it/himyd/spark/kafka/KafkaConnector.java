package it.himyd.spark.kafka;

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
	public final static String BROKERS = "localhost:9092";
	public final static String TOPICS = "stock_topic";

	private JavaStreamingContext jssc;
	private Map<String, String> kafkaParams;
	private Set<String> topicsSet;

	public KafkaConnector(JavaStreamingContext jssc) {
		this.jssc = jssc;

		this.kafkaParams = new HashMap<String, String>();
		this.kafkaParams.put("metadata.broker.list", BROKERS);

		this.topicsSet = new HashSet<String>();
		this.topicsSet.addAll(Arrays.asList(TOPICS.split(",")));

		System.out.println("Kafka Parameters Setted");
	}

	public KafkaConnector(JavaStreamingContext jssc, Map<String, String> kafkaParams, Set<String> topicsSet) {
		this.jssc = jssc;
		this.kafkaParams = kafkaParams;
		this.topicsSet = topicsSet;
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

}
