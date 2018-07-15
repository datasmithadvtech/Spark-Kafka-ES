package eventum.spark_trial;

import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.sum;

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
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import com.fasterxml.jackson.databind.ObjectMapper;

public class Streaming_using_DF_from_Kafka_JSON_insert_ES {

	public static void main(String[] args) throws InterruptedException {
		// Create a local StreamingContext with two working threads and batch interval
		// of 1 second
		// Number of threads should be > number of receivers(listening to the stream) to run, otherwise the
		// system will receive but will not be able to process it (except in file streaming which doesn't
		// need a receiver)
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("StreamingApp");
		
		// This configuration is set to serialize the ConsumerRecord class which is not serializable
		// Everything inside the forEachRDD method should be serializable as it will be
		// transferred across nodes
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.registerKryoClasses(new Class[] {ConsumerRecord.class});
		
		// Elasticsearch Configurations
		conf.set("es.index.auto.create", "true");
		conf.set("es.nodes", "localhost");
		conf.set("es.port","9200");
		conf.set("es.http.timeout","5m");
		conf.set("es.scroll.size","50");
		
		// The default behavior is retaining the grouping columns
		// We can change it by setting the below configuration to false
//		conf.set("spark.sql.retainGroupColumns", "false");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		//------------ Kafka configurations -------------------
		Map<String, Object> kafkaParams = new HashMap<>();
		
		// the addresses of the Kafka brokers in a "bootstrap" Kafka cluster that a Kafka 
		// client connects to initially to bootstrap itself.
		kafkaParams.put("bootstrap.servers", "localhost:9092,anotherhost:9092");
		
		// Message key and value deserializers
		kafkaParams.put("key.deserializer", LongDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		
		// name of the consumer group
		kafkaParams.put("group.id", "group_id");
		
		// What to do when there is no initial offset in Kafka 
		// or if the current offset does not exist any more on the server
		// latest: automatically reset the offset to the latest offset
		kafkaParams.put("auto.offset.reset", "latest");
		
		// Whether to commit the offset periodically automatically (true)
		// or to call commit method on the consumer manually (false)
		// In our case messages polled by the consumer may not yet have resulted in a Spark output yet
		// So we set the config to false and set the offsets manually after inserting data in ES (This is not managed yet)
//		kafkaParams.put("enable.auto.commit", false); 

		Collection<String> topics = Arrays.asList("Sandra");
		
		// Creating the stream from kafka
		JavaInputDStream<ConsumerRecord<Long, String>> stream = KafkaUtils.createDirectStream(jssc,
				LocationStrategies.PreferConsistent(), // related to the distribution of partitions
				ConsumerStrategies.<Long, String>Subscribe(topics, kafkaParams));
		
		
		// Creating the windowed stream
		// 1st argument is the window length, 2nd is the slide interval
		JavaDStream<ConsumerRecord<Long, String>> windowedStream = stream.window(new Duration(30000), new Duration(30000));
		
		// Loop over the windowed stream we made to map the records inside each RDD
		// to our business object (JSONOutputDTO)
		windowedStream.foreachRDD((rdd) -> {
			
			
			if(!rdd.isEmpty()) {
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
				
				// Dataset must be mapped to RDD<NetflowRecordDTO> to be inserted in ES
				// because the schema can't be inferred from the Dataset
				// Mapping the dataset to RDD<NetflowRecordDTO>
				JavaRDD<NetflowRecordDTO> recordsRDD = operatedOnDS.javaRDD().map((row) -> {
					NetflowRecordDTO netflowRecord = new NetflowRecordDTO();
					netflowRecord.setDestinationIP(row.getString(0));
					netflowRecord.setSourceIP(row.getString(1));
					netflowRecord.setRouterIP(row.getString(2));
					netflowRecord.setInInterface(row.getInt(3));
					netflowRecord.setOutInterface(row.getInt(4));
					netflowRecord.setMaxPacketsCount(row.getLong(5));
					netflowRecord.setSumOctetsCount(row.getLong(6));
					return netflowRecord;
				});
				
				System.out.println("Saving to ES.....");
				JavaEsSpark.saveToEs(recordsRDD, "netflow_records/records");
				
//				((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);
				
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
