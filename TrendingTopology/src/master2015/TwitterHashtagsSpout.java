package master2015;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.scribe.builder.ServiceBuilder;
import org.scribe.builder.api.TwitterApi;
import org.scribe.model.OAuthRequest;
import org.scribe.model.Response;
import org.scribe.model.Token;
import org.scribe.model.Verb;
import org.scribe.oauth.OAuthService;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.message.Message;

/**
 * 
 * TwitterHashtagsSpout: Storm Spout and Kafka Consumer
 * 
 * Receives messages from Kafka Consumer and send them to Storm Bolts.
 * 
 * @author Claudiu Barzu (claudiu.barzu@alumnos.upm.es)
 * @author Javier Villar Gil (javier.villar.gil@alumnos.upm.es)
 *
 */
public class TwitterHashtagsSpout extends BaseRichSpout {
	/** Zookeeper url for the Kafka Consumer **/
	private String zookeeper_url;

	/** Kafka Consumer Group ID **/
	public static final String KAFKA_GROUP_ID = "1";

	/** Kafka Consumer iterator **/
	private ConsumerIterator consumerIterator;

	/** Kafka Consumer Topic **/
	public static final String KAFKA_TOPIC = "TWITTER_GENERAL1";

	/** Storm Spout collector **/
	private SpoutOutputCollector collector;

	/** Storm Spout Language Output Field **/
	public static final String LANG_FIELD = "lang";

	/** Storm Spout Hashtags Output Field **/
	public static final String HASHTAGS_FIELD = "hashtags";

	/** Storm Spout Timestamp Output Field **/
	public static final String TIMESTAMP_FIELD = "timestamp";

	/** List with languages of interest **/
	private List<String> languagesList;

	// TODO remove
	private List<String> tweets;
	private Response response;
	private BufferedReader reader;

	private static final String STREAM_URI = "https://stream.twitter.com/1.1/statuses/sample.json";

	public TwitterHashtagsSpout(String zookeeper_url, List<String> languages) {
		this.zookeeper_url = zookeeper_url;
		this.languagesList = languages;
		this.readTweetsFromFile();
	}

	/** TODO Remove method **/
	private String getNextTweet() {
		if (this.tweets.size() > 0) {
			String tweet = this.tweets.get(0);
			this.tweets.remove(0);
			return tweet;
		}
		return "";
	}

	/** TODO Remove method **/
	private void readTweetsFromFile() {
		try {
			File archivo = new File("/home/javiervillargil/Desktop/prueba.txt");
			FileReader fr = new FileReader(archivo);
			BufferedReader br = new BufferedReader(fr);

			this.tweets = new ArrayList<String>();

			// Lectura del fichero
			String tweet;
			while ((tweet = br.readLine()) != null) {
				this.tweets.add(tweet);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;

		/** TODO Implement kafka consumer **/

		// ConsumerConnector kafkaConsumer = Consumer
		// .createJavaConsumerConnector(this.createConsumerConfig(this.zookeeper_url,
		// TwitterHashtagsSpout.KAFKA_GROUP_ID));
		//
		// Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		// topicCountMap.put(TwitterHashtagsSpout.KAFKA_TOPIC, 1);
		// Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap =
		// kafkaConsumer.createMessageStreams(topicCountMap);
		// List<KafkaStream<byte[], byte[]>> streams =
		// consumerMap.get(TwitterHashtagsSpout.KAFKA_TOPIC);
		// consumerIterator = streams.get(0).iterator();

		
		OAuthService service = new ServiceBuilder().provider(TwitterApi.class).apiKey("Y3VdpNIgRPorBp6uVpH2gvEZW")
				.apiSecret("2eMBf7AsZdLELJeF19HVyo4OZgGMdpHbVejFc3PKdjtIGatUVA").build();
		Token accessToken = new Token("4450825533-9drOs8cEBKnwBZqL07zpCg6tRMV3MIFq9Iox0U2",
				"s82Wai5tZjmuXBR9gbRp5eVMWHNZheYieXAIyMZcXQwCt");
		
		
		OAuthRequest request = new OAuthRequest(Verb.POST, STREAM_URI);
		request.addHeader("version", "HTTP/1.1");
		request.addHeader("host", "stream.twitter.com");
		request.setConnectionKeepAlive(true);
		request.addHeader("user-agent", "Twitter Stream Reader");
		// request.addBodyParameter("track", "java,heroku,twitter"); // Set
		// keywords you'd like to track here
		service.signRequest(accessToken, request);
		
		response = request.send();
		
		reader = new BufferedReader(new InputStreamReader(response.getStream()));
	}

	private String getMessage(Message message) {
		ByteBuffer buffer = message.payload();
		byte[] bytes = new byte[buffer.remaining()];
		buffer.get(bytes);
		return new String(bytes);
	}

	@Override
	public void nextTuple() {
		// if(consumerIterator.hasNext()){
		// Object o = consumerIterator.next().message();
		// if(o != null){
		// System.out.println(o.toString());
		// }
		// System.out.println("Received: "+new String((String)
		// consumerIterator.next().message()));
		// }

		///////////////////////////////////////////////////////////////


		
		

		// Create a reader to read Twitter's stream
		

		try {
			String line;
			while ((line = reader.readLine()) != null) {
				String tweet_json = line;

				if (tweet_json != null && !tweet_json.isEmpty()) { // tweet.isValid()
					// String tweet_json = this.getMessage(msg);

					// JSON parsing
					ObjectMapper om = new ObjectMapper();
					JsonNode rootNode;

					try {
						rootNode = om.readValue(tweet_json, JsonNode.class);

						String hashtagsList = "";

						for (JsonNode node : rootNode.get("entities").path("hashtags")) {
							hashtagsList = hashtagsList + node.get("text").toString() + "#";
						}

						if (!hashtagsList.isEmpty()) {
							hashtagsList = hashtagsList.substring(0, hashtagsList.length() - 1);
						}

						String lang = rootNode.get("lang").toString();

						String timeStamp = rootNode.get("timestamp_ms").toString();

						for (String validLanguage : this.languagesList) {
							if (validLanguage.equals(lang)) {
								Values value = new Values(timeStamp, lang, hashtagsList);
								// System.out.println("---> Storm SPOUT [" +
								// Top3App.SPOUT_ID + "] emiting ["+timeStamp +"
								// - "+lang +" - "+hashtagsList+"]........");
								collector.emit(Top3App.TWITTER_OUTSTREAM, value);
							}
						}
					} catch (IOException e) {
						e.printStackTrace();
					}

				}
			}
		} catch (Exception e) {

		}
		/////////////////////////////////////////////////////////////////

		/** TODO Change to Kafka consumer tweets **/
		/*
		 * String tweet_json = this.getNextTweet();
		 * 
		 * if (tweet_json != null && !tweet_json.isEmpty()) { // tweet.isValid()
		 * // String tweet_json = this.getMessage(msg);
		 * 
		 * //JSON parsing ObjectMapper om = new ObjectMapper(); JsonNode
		 * rootNode;
		 * 
		 * try { rootNode = om.readValue(tweet_json, JsonNode.class);
		 * 
		 * String hashtagsList = "";
		 * 
		 * for (JsonNode node : rootNode.get("entities").path("hashtags")) {
		 * hashtagsList = hashtagsList + node.get("text").toString() + "#"; }
		 * 
		 * if(!hashtagsList.isEmpty()){ hashtagsList = hashtagsList.substring(0,
		 * hashtagsList.length()-1); }
		 * 
		 * String lang = rootNode.get("lang").toString();
		 * 
		 * String timeStamp = rootNode.get("timestamp_ms").toString();
		 * 
		 * for (String validLanguage : this.languagesList) { if
		 * (validLanguage.equals(lang)) { Values value = new Values(timeStamp,
		 * lang, hashtagsList); //System.out.println("---> Storm SPOUT [" +
		 * Top3App.SPOUT_ID + "] emiting ["+timeStamp +" - "+lang +" - "
		 * +hashtagsList+"]........"); collector.emit(Top3App.TWITTER_OUTSTREAM,
		 * value); } } } catch (IOException e) { e.printStackTrace(); }
		 * 
		 * }
		 */
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(Top3App.TWITTER_OUTSTREAM, new Fields(TwitterHashtagsSpout.TIMESTAMP_FIELD,
				TwitterHashtagsSpout.LANG_FIELD, TwitterHashtagsSpout.HASHTAGS_FIELD));

	}

	private ConsumerConfig createConsumerConfig(String aZookeeper, String aGroupId) {

		Properties props = new Properties();

		props.put("zookeeper.connect", aZookeeper);
		props.put("group.id", aGroupId);
		props.put("zookeeper.session.timeout.ms", "400");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");

		return new ConsumerConfig(props);
	}

}
