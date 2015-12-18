package master;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * TwitterApp: Twitter reader and Kafka producer.
 * 
 * Reads tweets from a specific source (API or file) and send them to the Storm
 * topology
 * 
 * @author Claudiu Barzu (claudiu.barzu@alumnos.upm.es)
 * @author Javier Villar Gil (javier.villar.gil@alumnos.upm.es)
 *
 */
public class TwitterApp {
	/** ID for the Kafka Topic */
	public static final String KAFKA_TOPIC = "TWITTER_GENERAL";

	/**
	 * Main method
	 * 
	 * @param args[0]
	 *            mode. 1 means read from file, 2 read from the Twitter API.
	 * @param args[1]
	 *            apiKey. Key associated with the Twitter app consumer.
	 * @param args[2]
	 *            apiSecret. Secret associated with the Twitter app consumer.
	 * @param args[3]
	 *            tokenValue. Access token associated with the Twitter app.
	 * @param args[4]
	 *            tokenSecret. Access token secret.
	 * @param args[5]
	 *            Kafka Broker URL. String in the format IP:port corresponding
	 *            with the Kafka Broker
	 * @param args[6]
	 *            Filename: path to the file with the tweets (the path is
	 *            related to the filesystem of the node that will be used to run
	 *            the Twitter app).
	 */
	public static void main(String[] args) {

		/** Reads and verifies arguments **/
		if (args.length != 7) {
			System.err.println("****** ERROR ****** Wrong arguments. Usage:"
					+ "\n\t[1] mode: 1 means read from file, 2 read from the Twitter API."
					+ "\n\t[2] apiKey: key associated with the Twitter app consumer."
					+ "\n\t[3] apiSecret: secret associated with the Twitter app consumer."
					+ "\n\t[4] tokenValue: access token associated with the Twitter app."
					+ "\n\t[5] tokenSecret: access token secret."
					+ "\n\t[6] Kafka Broker URL: String in the format IP:port corresponding with the Kafka Broker"
					+ "\n\t[7] Filename: path to the file with the tweets (the path is related to the filesystem of the node that will be used to run the Twitter app)");
			System.exit(-1);
		}

		System.out.println(" ----------------------------------------");
		System.out.println("            STARTING TwitterApp          ");
		System.out.println(" ----------------------------------------");
		System.out.println("\nGiven arguments:");
		System.out.println("\t[1] mode: " + args[0]);
		System.out.println("\t[2] apiKey: " + args[1]);
		System.out.println("\t[3] apiSecret: " + args[2]);
		System.out.println("\t[4] tokenValue: " + args[3]);
		System.out.println("\t[5] tokenSecret: " + args[4]);
		System.out.println("\t[6] Kafka Broker URL: " + args[5]);
		System.out.println("\t[7] Filename: " + args[6]);

		/** Configuration and creation of the kafka producer **/
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, args[5]);
		props.put(ProducerConfig.RETRIES_CONFIG, "3");
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 200);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

		/** Call the reader (depending on mode arg) **/
		int mode = 0;
		try {
			mode = Integer.parseInt(args[0]);
		} catch (NumberFormatException e) {
			System.err.println("Wrong mode argument");
			System.exit(-2);
		}

		switch (mode) {
		case 1:
			new TweetReaderFile(producer, args[6]);
			break;
		case 2:
			new TweetReaderAPI(producer, args[1], args[2], args[3], args[4]);
			break;
		}

	}
}
