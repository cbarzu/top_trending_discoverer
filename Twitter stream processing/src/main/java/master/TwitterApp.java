package master;

import org.apache.kafka.clients.producer.KafkaProducer;

public class TwitterApp {
	public static void main(String[] args) {
		KafkaProducer<String, String> producer = null;
		
		System.out.println("Simple Twitter App with param "+args[0]);
	}
}
