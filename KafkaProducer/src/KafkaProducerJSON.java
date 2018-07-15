
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class KafkaProducerJSON {

	private final static String TOPIC = "Sandra";
	private final static String BOOTSTRAP_SERVERS = "localhost:9092";
	private static String[] IPs = {"127.60.82.5", "127.60.90.5", "127.102.90.3"};

	public static void main(String[] args) throws Exception {
		runProducer(1000000);
	}

	public static Producer<Long, String> createProducer() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "kafkaExampleProducer");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		return new KafkaProducer<>(props);
	}

	static void runProducer(final int sendMessageCount) throws Exception {
		final Producer<Long, String> producer = createProducer();
		long time = System.currentTimeMillis();
		final CountDownLatch countDownLatch = new CountDownLatch(sendMessageCount);


		try {
			Long counter = (long) 0;
			while (counter < sendMessageCount) {
				counter++;
				// Creating the json object to be sent
				JSONOutputDTO jsonObj = new JSONOutputDTO();
				jsonObj.setDestinationIP(IPs[(int) (Math.random()*2)]);
				jsonObj.setSourceIP(IPs[(int) (Math.random()*2)]);
				jsonObj.setRouterIP(IPs[(int) (Math.random()*2)]);
				jsonObj.setInInterface((int) Math.round(Math.random()*2));
				jsonObj.setOutInterface((int) Math.round(Math.random()*2));
				jsonObj.setPacketsCount((long) (counter*Math.random()*10));
				jsonObj.setOctetsCount((long) (counter*Math.random()*1000));
				jsonObj.setRecordKey(counter);
				jsonObj.setTimestamp(new Timestamp(System.currentTimeMillis()));
				String jsonString = new ObjectMapper().writeValueAsString(jsonObj);
				
				// Creating the producer record to be sent
				final ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(TOPIC, counter,
						jsonString);

				// he response RecordMetadata has ‘partition’ where the record
				// was written and the ‘offset’ of the record in that partition.
				
				// the second argument on send is a callback method
				// The callback interface allows code to execute when the request is complete.
				producer.send(record, (metadata, exception) -> {
					if(metadata != null) {
						long elapsedTime = System.currentTimeMillis() - time;
						System.out.printf("sent record(key=%d value=%s)" + "meta(partition=%d, offset=%d) time=%d\n",
						record.key(), record.value(), metadata.partition(), metadata.offset(), elapsedTime);
						if((System.currentTimeMillis() - time) >= 60000) {
						}
					} else {
						exception.printStackTrace();
					}
					countDownLatch.countDown();
				});
				
//				Thread.sleep(5000);
			}
			countDownLatch.await(25, TimeUnit.SECONDS);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// the call to flush and close. Kafka will auto flush on its own,
			// but you can also call flush explicitly which will send the accumulated
			// records now.
			producer.flush();
			producer.close();
			double totalTimeInSeconds = (System.currentTimeMillis() - time)/1000.0;
			System.out.println("Records sent per seconds = " + (sendMessageCount)/totalTimeInSeconds);

		}
	}

}
