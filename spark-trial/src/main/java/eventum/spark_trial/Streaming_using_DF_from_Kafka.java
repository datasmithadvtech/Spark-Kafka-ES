package eventum.spark_trial;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

public class Streaming_using_DF_from_Kafka {

	public static void main(String[] args) throws InterruptedException {
		// Create a local StreamingContext with two working threads and batch interval
		// of 1 second
		// Number of threads should be > number of receivers to run, otherwise the
		// system will receive
		// but will not be able to process it (except in file streaming which doesn't
		// need a receiver)
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("StreamingApp");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		// Map<String, Object> kafkaParams = new HashMap<String, Object>();
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "localhost:9092,anotherhost:9092");
		kafkaParams.put("key.deserializer", LongDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "group_id");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);

		Collection<String> topics = Arrays.asList("Sandra");

		// Creating the stream from kafka
		JavaInputDStream<ConsumerRecord<Long, String>> stream = KafkaUtils.createDirectStream(jssc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<Long, String>Subscribe(topics, kafkaParams));

		stream.foreachRDD((rdd) -> {
			// Get the singleton instance of SparkSession
			SparkSession spark = SparkSession.builder().config(rdd.context().getConf()).getOrCreate();

			// Convert RDD[String] to RDD[case class} to DataFrame
			JavaRDD<JavaRow> rowRDD = rdd.map((consumerRecord) -> {
				JavaRow javaRow = new JavaRow();
				javaRow.setWord(consumerRecord.value());
				javaRow.setRecordKey(consumerRecord.key());
				return javaRow;
			});
			Dataset<Row> wordsDataFrame = spark.createDataFrame(rowRDD, JavaRow.class);

			if(!rdd.isEmpty()) {
				wordsDataFrame.show();
			}
		});
		
		// When these lines are executed, Spark Streaming only sets up the computation
		// it will perform after it is started
		// and no real processing has started yet. To start the processing after all the
		// transformations have been setup,
		// we finally call start method.
		jssc.start();
		jssc.awaitTermination();

		jssc.close();
	}

}
