package master2015;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

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



	public TwitterHashtagsSpout(String zookeeper_url, List<String> languages) {
		this.zookeeper_url = zookeeper_url;
		this.languagesList = languages;
		this.readTweetsFromFile();
	}

	/** TODO Remove method **/
	private String getRandomTweet() {
		int index = new Random().nextInt(this.tweets.size());
		return this.tweets.get(index);
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
		
		/**TODO Change to Kafka consumer tweets **/
		String tweet_json = this.getRandomTweet();

		if (tweet_json != null && !tweet_json.isEmpty()) { // tweet.isValid()
			// String tweet_json = this.getMessage(msg);

			//JSON parsing
			ObjectMapper om = new ObjectMapper();
			JsonNode rootNode;

			try {
				rootNode = om.readValue(tweet_json, JsonNode.class);

				String hashtagsList = "";

				for (JsonNode node : rootNode.get("entities").path("hashtags")) {
					hashtagsList = hashtagsList + node.get("text").toString() + "#";
				}

				String lang = rootNode.get("lang").toString();

				String timeStamp = rootNode.get("timestamp_ms").toString();
				
				for (String validLanguage : this.languagesList) {
					if (validLanguage.equals(lang)) {
						Values value = new Values(timeStamp, lang, hashtagsList);
						//System.out.println("---> Storm SPOUT [" + Top3App.SPOUT_ID + "] emiting ["+timeStamp +" - "+lang +" - "+hashtagsList+"]........");
						collector.emit(Top3App.TWITTER_OUTSTREAM, value);
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}

		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(Top3App.TWITTER_OUTSTREAM,
				new Fields(TwitterHashtagsSpout.TIMESTAMP_FIELD, TwitterHashtagsSpout.LANG_FIELD, TwitterHashtagsSpout.HASHTAGS_FIELD));

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
