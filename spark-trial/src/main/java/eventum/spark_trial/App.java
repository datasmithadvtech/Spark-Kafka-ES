package eventum.spark_trial;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class App 
{
    public static void main( String[] args )
    {
    	// Spark Session
    	SparkSession spark  = SparkSession
    			.builder()
    			.appName("spark-trial")
    			.config("spark.master", "local")
    			.getOrCreate();
    	
 
    	// Creating a DataFrame based on the content of a JSON file
        Dataset<Row> df = spark.read().json("src/main/resources/people.json");
        df.show();
        
        // Reading a readme file 
        Dataset<String> logData = spark.read().textFile("D:\\Sources\\spark-2.3.1-bin-hadoop2.7\\spark-2.3.1-bin-hadoop2.7\\README.md").cache();
        
        long numAs = logData.filter(s -> s.contains("a")).count();
        long numBs = logData.filter(s -> s.contains("b")).count();
        
        System.out.println("lines with a: " + numAs + ", lines with b: " + numBs);
        
        // Closing the session
        spark.close();
        
    }
}
