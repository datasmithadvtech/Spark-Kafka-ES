package eventum.spark_trial;


import java.util.ArrayList;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class PartitioningAndGloming 
{
    public static void main( String[] args )
    {
    	// Spark Session
    	SparkSession spark  = SparkSession
    			.builder()
    			.appName("spark-trial")
    			.config("spark.master", "local")
    			.getOrCreate();
    	      
        
        SparkContext sc = spark.sparkContext();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);
        
        ArrayList<Integer> lotsOfNumbers = new ArrayList<>();
        
        for (int i = 0; i < 100; i++) {
			lotsOfNumbers.add(i);
		}
        
        JavaRDD<Integer> a = jsc.parallelize(lotsOfNumbers, 10);
        System.out.println("Partitions: "  + a.getNumPartitions());
        
        // glom() returns a list of the RDDs in all the partitions
        System.out.println(a.glom().map((p) -> p.size()).collect());
        
        // now when we filter there will be 8 empty partitions
        System.out.println("Filtering ....");
        JavaRDD<Integer> b = a.filter((value) ->  value >= 80);
        System.out.println(b.glom().map((p) -> p.size()).collect());
        
        // To fix this we use repartition method
        System.out.println("Repartitioning.....");
        JavaRDD<Integer> c = b.repartition(10);
        System.out.println(c.glom().map((p) -> p.size()).collect());

        
        // Closing the session
        spark.close();
        
    }
}
