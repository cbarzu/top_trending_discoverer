package master;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * TweetReaderFile: Reader from tweet file.
 * 
 * Reads the tweets from a source file and send them to a kafka producer.
 * 
 * @author Claudiu Barzu (claudiu.barzu@alumnos.upm.es)
 * @author Javier Villar Gil (javier.villar.gil@alumnos.upm.es)
 *
 */
public class TweetReaderFile {
	private KafkaProducer<String, String> producer;
	private String filePath;

	public TweetReaderFile(KafkaProducer<String, String> producer, String filePath) {
		this.producer = producer;
		this.filePath = filePath;

		this.readTweetsFile();
	}

	private void readTweetsFile() {
		try {
			File archivo = new File(filePath);
			FileReader fr = new FileReader(archivo);
			BufferedReader br = new BufferedReader(fr);

			// Tweet file read
			String tweet;
			while ((tweet = br.readLine()) != null) {
				System.out
						.println("---> [" + new Date().toString() + "]Kafka producer: emitting a new tweet :" + tweet);
				producer.send(new ProducerRecord<String, String>(TwitterApp.KAFKA_TOPIC, tweet));
			}

			if (null != fr) {
				fr.close();
			}
		} catch (IOException e) {
			System.err.println("---> Kafka producer ERROR: " + e.getMessage());
			e.printStackTrace();
		}
	}
}
