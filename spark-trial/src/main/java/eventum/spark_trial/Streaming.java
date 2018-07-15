package eventum.spark_trial;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import scala.Tuple2;

public class Streaming {

	public static void main(String[] args) throws InterruptedException {
		// Create a local StreamingContext with two working threads and batch interval of 1 second
		// Number of threads should be > number of receivers to run, otherwise the system will receive 
		// but will not be able to process it (except in file streaming which doesn't need a receiver)
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("StreamingApp");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
		
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		
		
		// create DStream that represents streaming data from a TCP source, specified as localhost:9999
		JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
		
		// DStream represents the stream of data that will be received from the data server. 
		// Each record in this stream is a line of text
		// Every input DStream is associated with a Receiver object which receives the data
		// from a source and stores it in Spark's memory for processing
		// among these sources is Kafka
		
		// Split each line into words
		JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
		// flatMap is a DStream operation that creates a new DStream 
		// by generating multiple new records from each record in the source DStream
		
		// Count each word in each batch
		JavaPairDStream<String, Integer> pairs = words.mapToPair((s) -> new Tuple2<>(s, 1));
		
		// The reduceByKey function adds the 1's of each word (which is the key) 
		// produces a tuple where the key is the word and the value is the count
		JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((v1, v2) -> v1 + v2);
		
		// Print the first ten elements of each RDD generated in this DStream to the console
		System.out.println("Printing words count....");
		wordCounts.print();
		
		// When these lines are executed, Spark Streaming only sets up the computation it will perform after it is started
		// and no real processing has started yet. To start the processing after all the transformations have been setup, 
		// we finally call start method.
		jssc.start();
		jssc.awaitTermination();
		

		
		jssc.close();
	}

}
