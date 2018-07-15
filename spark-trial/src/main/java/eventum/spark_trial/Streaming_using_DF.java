package eventum.spark_trial;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class Streaming_using_DF {

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

		// Split each line into words
		JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
		// flatMap is a DStream operation that creates a new DStream 
		// by generating multiple new records from each record in the source DStream
		
		words.foreachRDD((rdd, time) -> {
			// Get the singleton instance of SparkSession
			SparkSession spark = SparkSession.builder().config(rdd.context().getConf()).getOrCreate();
			
			// Convert RDD[String] to RDD[case class} to DataFrame
			JavaRDD<JavaRow> rowRDD = rdd.map((word) -> {
				JavaRow record = new JavaRow();
				record.setWord(word);
				return record;
			});
			
			Dataset<Row> wordsDataFrame = spark.createDataFrame(rowRDD, JavaRow.class);
			
			System.out.println("Prinitng the words dataset.....");
			wordsDataFrame.show();
			
			 // Creates a temporary view using the DataFrame
			 wordsDataFrame.createOrReplaceTempView("words");
			 
			 // Do word count on table using SQL and print it
			 Dataset<Row> wordCountDataFrame = spark.sql("select word, count(*) as total from words group by word");
			 
			 System.out.println("Printing word count.....");
			 wordCountDataFrame.show();
		});
		
		
		// When these lines are executed, Spark Streaming only sets up the computation it will perform after it is started
		// and no real processing has started yet. To start the processing after all the transformations have been setup, 
		// we finally call start method.
		jssc.start();
		jssc.awaitTermination();
		

		
		jssc.close();
	}

}
