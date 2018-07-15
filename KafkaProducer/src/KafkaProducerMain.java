
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaProducerMain {

	private final static String TOPIC = "Sandra";
	private final static String BOOTSTRAP_SERVERS = "localhost:9092";

	public static void main(String[] args) throws Exception {
		runProducer(5);
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
			for (long index = time; index < time + sendMessageCount; index++) {
				final ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(TOPIC, index,
						"Hello there " + index);

				// he response RecordMetadata has ‘partition’ where the record
				// was written and the ‘offset’ of the record in that partition.
				
				// the second argument on send is a callback method
				// The callback interface allows code to execute when the request is complete.
				Future<RecordMetadata> future = producer.send(record, (metadata, exception) -> {
					if(metadata != null) {
						long elapsedTime = System.currentTimeMillis() - time;
						System.out.printf("sent record(key=%d value=%s)" + "meta(partition=%d, offset=%d) time=%d\n",
						record.key(), record.value(), metadata.partition(), metadata.offset(), elapsedTime);
					} else {
						exception.printStackTrace();
					}
					countDownLatch.countDown();
				});
//				RecordMetadata metadata = future.get();
			}
			// use of CountDownLatch so we can send all N messages and then wait for them all to send.
			countDownLatch.await(25, TimeUnit.SECONDS);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// the call to flush and close. Kafka will auto flush on its own,
			// but you can also call flush explicitly which will send the accumulated
			// records now.
			producer.flush();
			producer.close();
		}
	}

}
