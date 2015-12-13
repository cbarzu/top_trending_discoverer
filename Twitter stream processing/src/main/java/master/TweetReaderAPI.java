package master;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

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


public class TweetReaderAPI extends Thread {

    private static final String STREAM_URI = "https://stream.twitter.com/1.1/statuses/sample.json";
    Logger log = Logger.getLogger(TweetReaderAPI.class);
	private OAuthService service;
	private Token accessToken;
	private KafkaProducer producer;
	
	public TweetReaderAPI(KafkaProducer prod, String apiKey, String apiSecret, String tokenValue, String tokenSecret){
		service = new ServiceBuilder().provider(TwitterApi.class).apiKey(apiKey).apiSecret(apiSecret).build();
		accessToken = new Token(tokenValue,tokenSecret);
		producer = prod;
	}
	
	  public void run(){
	        try{
	        	log.info("Starting Twitter public stream consumer thread.");

	            // Let's generate the request
	            log.info("Connecting to Twitter Public Stream");
	            OAuthRequest request = new OAuthRequest(Verb.POST, STREAM_URI);
	            request.addHeader("version", "HTTP/1.1");
	            request.addHeader("host", "stream.twitter.com");
	            request.setConnectionKeepAlive(true);
	            request.addHeader("user-agent", "Twitter Stream Reader");
	            //request.addBodyParameter("track", "java,heroku,twitter"); // Set keywords you'd like to track here
	            service.signRequest(accessToken, request);
	            Response response = request.send();

	            // Create a reader to read Twitter's stream
	            BufferedReader reader = new BufferedReader(new InputStreamReader(response.getStream()));

	            		
	            String line;
	            while ((line = reader.readLine()) != null) {
	            	producer.send(new ProducerRecord<String, String>(TwitterApp.KAFKA_TOPIC, line));
	                System.out.println(line);
	            }
	        }
	        catch (IOException ioe){
	            ioe.printStackTrace();
	        }
	  }

}
