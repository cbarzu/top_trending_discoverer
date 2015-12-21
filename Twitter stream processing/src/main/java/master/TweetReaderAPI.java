package master;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import org.scribe.builder.ServiceBuilder;
import org.scribe.builder.api.TwitterApi;
import org.scribe.model.OAuthRequest;
import org.scribe.model.Response;
import org.scribe.model.Token;
import org.scribe.model.Verb;
import org.scribe.oauth.OAuthService;

/**
 * TweetReaderAPI: Reader from Twitter API.
 * 
 * Reads the tweets from the Twitter API and send them to a kafka producer.
 * 
 * @author Claudiu Barzu (claudiu.barzu@alumnos.upm.es)
 * @author Javier Villar Gil (javier.villar.gil@alumnos.upm.es)
 *
 */
public class TweetReaderAPI extends Thread {

	private static final String STREAM_URI = "https://stream.twitter.com/1.1/statuses/filter.json";
	Logger log = Logger.getLogger(TweetReaderAPI.class);
	private OAuthService service;
	private Token accessToken;
	private KafkaProducer<String, String> producer;

	public TweetReaderAPI(KafkaProducer<String, String> prod, String apiKey, String apiSecret, String tokenValue,
			String tokenSecret) {
		service = new ServiceBuilder().provider(TwitterApi.class).apiKey(apiKey).apiSecret(apiSecret).build();
		accessToken = new Token(tokenValue, tokenSecret);
		producer = prod;

		readTweetsFromApi();
	}

	public void readTweetsFromApi() {
		/** Twitter API connection **/
		try {
			System.out.println("---> TweetReaderAPI: Starting Twitter public stream consumer thread ........");
			System.out.println("---> TweetReaderAPI: Connecting to Twitter Public Stream ........");
			OAuthRequest request = new OAuthRequest(Verb.POST, STREAM_URI);
			request.addHeader("version", "HTTP/1.1");
			request.addHeader("host", "stream.twitter.com");
			request.addBodyParameter("track", "car,elecciones,football,please,android,romania,revelion,navidad");
			request.setConnectionKeepAlive(true);
			request.addHeader("user-agent", "Twitter Stream Reader");
			service.signRequest(accessToken, request);
			Response response = request.send();

			// Create a reader to read Twitter's stream
			BufferedReader reader = new BufferedReader(new InputStreamReader(response.getStream()));

			String line;
			while ((line = reader.readLine()) != null) {
				try {
					producer.send(new ProducerRecord<String, String>(TwitterApp.KAFKA_TOPIC, line)).get();
				} catch (InterruptedException | ExecutionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.out.println("---> [" + new Date().toString() + "]Kafka producer: emitting a new tweet: " + line);
			}
		} catch (IOException ioe) {
			System.err.println("---> Kafka producer ERROR: " + ioe.getMessage());
			ioe.printStackTrace();
		}
	}

}
