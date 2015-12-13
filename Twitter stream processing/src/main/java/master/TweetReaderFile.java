package master;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class TweetReaderFile {
	private KafkaProducer producer;
	private String filePath;
	
	public TweetReaderFile(KafkaProducer producer, String filePath) {
		this.producer = producer;
		this.filePath = filePath;
		
		this.readTweetsFile();
	}
	
	private void readTweetsFile(){
		//TODO Lectura de los tweets del fichero
		
		String tweet = "";
		
		producer.send(new ProducerRecord<K, V>(topic, tweet));
	}
}
