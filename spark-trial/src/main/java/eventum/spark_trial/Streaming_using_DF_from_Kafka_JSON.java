package eventum.spark_trial;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import static org.apache.spark.sql.functions.*;

import com.fasterxml.jackson.databind.ObjectMapper;

public class Streaming_using_DF_from_Kafka_JSON {

	public static void main(String[] args) throws InterruptedException {
		// Create a local StreamingContext with two working threads and batch interval
		// of 1 second
		// Number of threads should be > number of receivers(listening to the stream) to run, otherwise the
		// system will receive but will not be able to process it (except in file streaming which doesn't
		// need a receiver)
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("StreamingApp");
		
		// This configuration is set to serialize the ConsumerRecord class which is not serializable
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.registerKryoClasses(new Class[] {ConsumerRecord.class});
		
		// The default behavior is retaining the grouping columns
		// We can change it by setting the below configuration to false
//		conf.set("spark.sql.retainGroupColumns", "false");
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
		

		/*
		JavaDStream<JSONOutputDTO> recordsStream = stream.window(new Duration(30000)).map((consumerRecord) -> {
			// Creating json object
			System.out.println("Parsing JSON.....");
			JSONOutputDTO outputJSON = new ObjectMapper().readValue(consumerRecord.value(), JSONOutputDTO.class);
			
			
			return outputJSON;
		});
		
		recordsStream.foreachRDD(rdd -> {
			// Get the singleton instance of SparkSession
			SparkSession spark = SparkSession.builder().config(rdd.context().getConf()).getOrCreate();
			Dataset<Row> wordsDataFrame = spark.createDataFrame(rdd, JSONOutputDTO.class);

			if(!rdd.isEmpty()) {
				wordsDataFrame.show();
			}
		});
		*/

		// 1st argument is the window length, 2nd is the slide interval
		JavaDStream<ConsumerRecord<Long, String>> windowedStream = stream.window(new Duration(100000), new Duration(20000));
		
		// Loop over the windowed stream we made to map the records inside each RDD
		// to our business object (JSONOutputDTO)
		windowedStream.foreachRDD((rdd) -> {
			// Get the singleton instance of SparkSession
			SparkSession spark = SparkSession.builder().config(rdd.context().getConf()).getOrCreate();
			

			// Looping over the records of each RDD to 
			// convert RDD[ConsumerRecord] to RDD[JSONObj}
			JavaRDD<JSONOutputDTO> mappedRdd = rdd.map((consumerRecord) -> {
				// Creating json object
				System.out.println("Parsing JSON of record: " + consumerRecord.key());
				JSONOutputDTO outputJSON = new ObjectMapper().readValue(consumerRecord.value(), JSONOutputDTO.class);
				
				return outputJSON;
			}).cache();
			// cache is added so that the mapping processing would take place only once and be cached
			
			// Creating the dataset to compute on from our mapped RDD
			Dataset<Row> windowDataFrame = spark.createDataFrame(mappedRdd, JSONOutputDTO.class);
			
			System.out.println("The table operated on....");
			System.out.println("Count = "+ windowDataFrame.count());
			windowDataFrame.show();
			
			// Do operations on the dataset
			System.out.println("The table after grouping and aggregations....");
			Dataset<Row> operatedOnDS = windowDataFrame.groupBy("destinationIP", "sourceIP", "routerIP", "inInterface", "outInterface")
					.agg(max(windowDataFrame.col("packetsCount")), sum("octetsCount"));
			System.out.println("Count = " + operatedOnDS.count());
			operatedOnDS.show();
//			windowDataFrame.groupBy("destinationIP").sum("octetsCount").show();
			
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
