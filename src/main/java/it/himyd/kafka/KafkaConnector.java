package it.himyd.kafka;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import it.himyd.stock.StockOHLC;
import kafka.serializer.StringDecoder;

public class KafkaConnector implements Serializable {
	private static final long serialVersionUID = 1L;

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

	public void writeOHLC(JavaDStream<StockOHLC> ohlc) {
		ohlc.foreachRDD(rdd -> {
			// wordCountStream.foreachRDD( rdd => {
			System.out.println("# events = " + rdd.count());
			// Print statements in this section are shown in the executor's stdout logs
			String kafkaOpTopic = "stock-ohlc";
			HashMap<String, Object> props = new HashMap<String, Object>();
			props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
			props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
					"org.apache.kafka.common.serialization.StringSerializer");
			props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
					"org.apache.kafka.common.serialization.StringSerializer");
			KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
			// TODO: can avoid collect?
			rdd.collect().forEach(record -> {
				String data = record.toString();
				// As as debugging technique, users can write to DBFS to verify that records are being written out
				// dbutils.fs.put("/tmp/test_kafka_output",data,true)
				ProducerRecord<String, String> message = new ProducerRecord<String, String>(kafkaOpTopic, null, data);
				producer.send(message);
			});
			producer.close();
		});
	}

	// TODO: not serializable
	public void writeOHLC2(JavaDStream<StockOHLC> ohlc) {
		ohlc.foreachRDD(rdd -> {
			// wordCountStream.foreachRDD( rdd => {
			System.out.println("# events = " + rdd.count());
			rdd.foreachPartition(partition -> {
				// Print statements in this section are shown in the executor's stdout logs
				String kafkaOpTopic = "stock-ohlc";
				HashMap<String, Object> props = new HashMap<String, Object>();
				props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokerAddress);
				props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
						"org.apache.kafka.common.serialization.StringSerializer");
				props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
						"org.apache.kafka.common.serialization.StringSerializer");
				KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
				partition.forEachRemaining(record -> {
					String data = record.toString();
					// As as debugging technique, users can write to DBFS to verify that records are being written out
					// dbutils.fs.put("/tmp/test_kafka_output",data,true)
					ProducerRecord<String, String> message = new ProducerRecord<String, String>(kafkaOpTopic, null,
							data);
					System.out.println(message.value());
					producer.send(message);
				});
				producer.close();
			});
		});
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
