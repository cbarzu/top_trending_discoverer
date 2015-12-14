package master;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

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

	private void readTweetsFile() {
		// TODO Lectura de los tweets del fichero
		try {
			File archivo = new File(filePath);
			FileReader fr = new FileReader(archivo);
			BufferedReader br = new BufferedReader(fr);

			// Lectura del fichero
			String tweet;
			while ((tweet = br.readLine()) != null) {
                System.out.println("emitting "+tweet);
				producer.send(new ProducerRecord<String, String>(TwitterApp.KAFKA_TOPIC, tweet));
			}
			
		
			if (null != fr) {
				fr.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
