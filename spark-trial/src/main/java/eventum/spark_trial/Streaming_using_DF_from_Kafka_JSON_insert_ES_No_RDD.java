package eventum.spark_trial;

import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.window;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.streaming.StreamExecution;
import org.apache.spark.sql.execution.streaming.StreamingQueryWrapper;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.StreamingQueryProgress;
import org.apache.spark.sql.streaming.Trigger;

import com.fasterxml.jackson.databind.ObjectMapper;

public class Streaming_using_DF_from_Kafka_JSON_insert_ES_No_RDD {

	public static void main(String[] args) throws InterruptedException {
		// Create a local StreamingContext with two working threads and batch interval
		// of 1 second
		// Number of threads should be > number of receivers(listening to the stream) to
		// run, otherwise the
		// system will receive but will not be able to process it (except in file
		// streaming which doesn't
		// need a receiver)
		SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("StreamingApp");

		// This configuration is set to serialize the ConsumerRecord class which is not
		// serializable
		// Everything inside the forEachRDD method should be serializable as it will be
		// transferred across nodes
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.registerKryoClasses(new Class[] { ConsumerRecord.class });

		// Elasticsearch Configurations
		conf.set("es.index.auto.create", "true");
		conf.set("es.nodes", "localhost");
		conf.set("es.port", "9200");
		conf.set("es.http.timeout", "5m");
		conf.set("es.scroll.size", "50");
		// conf.set("spark.streaming.receiver.maxRate", "100");
		conf.set("spark.sql.shuffle.partitions", "50");

		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		// Load Dataset from stream
		Dataset<Row> loadedDS = spark.readStream().format("kafka")
				.option("kafka.bootstrap.servers", "localhost:9092,localhost:9092")
				.option("subscribe", "Sandra")
				.option("startingOffsets", "earliest")
				.load()
				.select("value") // Selecting a set of SQL expressions
				.selectExpr("CAST(value AS STRING)");

		// Map Dataset<Row> to Dataset<JSONOutputJSON> which is the netflow record
		Dataset<JSONOutputDTO> recordsDS = loadedDS.map((row) -> {
			JSONOutputDTO outputJSON = new ObjectMapper().readValue(row.getString(0), JSONOutputDTO.class);

			// System.out.println("Object being mapped: " + outputJSON.getRecordKey());
			return outputJSON;
		}, Encoders.bean(JSONOutputDTO.class)).withWatermark("timestamp", "10 minutes");

		// Group the records by the window and the columns + aggregate
		Dataset<Row> aggregatedRecords = recordsDS
				.groupBy(window(recordsDS.col("timestamp"), "30 seconds", "30 seconds"), recordsDS.col("destinationIP"),
						recordsDS.col("sourceIP"), recordsDS.col("routerIP"), recordsDS.col("inInterface"),
						recordsDS.col("outInterface"))
				.agg(max(recordsDS.col("packetsCount")), sum("octetsCount"), count("destinationIP").alias("Count"));

		/*
		 * Complete Mode - The entire updated Result Table will be written to the
		 * external storage 
		 * Append Mode - Only the new rows appended in the Result Table
		 * since the last trigger will be written to the external storage 
		 * Update Mode - Only the rows that were updated in the Result Table since the last trigger
		 * will be written to the external storage
		 */

		// Start running the query that inserts into elasticsearch
		 StreamingQuery query =
				 aggregatedRecords
				 .writeStream()
				 .outputMode(OutputMode.Append())
				 .format("org.elasticsearch.spark.sql")
				 .option("checkpointLocation", "/tmp") // Elasticsearch commit log
				 .trigger(Trigger.ProcessingTime(5, TimeUnit.SECONDS))
				 .start("netflow_records/records");

//		StreamingQuery query = aggregatedRecords
//				.writeStream()
//				.outputMode("update")
//				.trigger(Trigger.ProcessingTime(0, TimeUnit.MINUTES)) // Every trigger interval, new rows get appended
//																		// to the Input Table
//				.format("console").start();

		Thread thread = new Thread() {
			public void run() {
				ArrayList<Long> printedBatches = new ArrayList<>();
				while (true) {
					StreamExecution streamExecution = ((StreamingQueryWrapper) query).streamingQuery();
					StreamingQueryProgress lastStreamingQueryProgress = streamExecution.lastProgress();
					if (lastStreamingQueryProgress != null) {
						 if(!printedBatches.contains(lastStreamingQueryProgress.batchId())) {
							
							printedBatches.add(lastStreamingQueryProgress.batchId());
							int totalQueryDuration = 0;
							for (long d : lastStreamingQueryProgress.durationMs().values()) {
								totalQueryDuration += d;
							}
							System.out.println("Batch " + lastStreamingQueryProgress.batchId());
							System.out.println("Query duration = " + totalQueryDuration / 1000 + "seconds");
							System.out.println("Total number of records = " + lastStreamingQueryProgress.numInputRows());
							System.out.println("Rows processed per second = " + lastStreamingQueryProgress.processedRowsPerSecond());
							System.out.println(lastStreamingQueryProgress.json());
						 }
					}
					System.out.println(streamExecution.currentStatus().message());

					try {
						Thread.sleep(500);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		};
		thread.start();

		try {
			query.awaitTermination();
		} catch (StreamingQueryException e) {
			e.printStackTrace();
		}
	}

}
