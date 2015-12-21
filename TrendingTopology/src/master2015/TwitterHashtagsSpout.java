package master2015;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	private ConsumerIterator <byte[], byte[]> consumerIterator;

	/** Kafka Consumer Topic **/
	public static final String KAFKA_TOPIC = "TWITTER_GENERAL";

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

	public static Logger LOG = LoggerFactory.getLogger(TwitterHashtagsSpout.class);

	public TwitterHashtagsSpout(String zookeeper_url, List<String> languages) {
		this.zookeeper_url = zookeeper_url;
		this.languagesList = languages;

	}

	

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;

		/** TODO Implement kafka consumer **/
		
		ConsumerConnector kafkaConsumer =  Consumer.createJavaConsumerConnector(this.createConsumerConfig(this.zookeeper_url,
		TwitterHashtagsSpout.KAFKA_GROUP_ID));
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(TwitterHashtagsSpout.KAFKA_TOPIC, 1);
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap =
		(Map<String, List<KafkaStream<byte[], byte[]>>>) kafkaConsumer.createMessageStreams(topicCountMap);
		List<KafkaStream<byte[], byte[]>> streams =
		consumerMap.get(TwitterHashtagsSpout.KAFKA_TOPIC);
		consumerIterator = streams.get(0).iterator();
		LOG.info("Created consumer iterator");

	}

	@Override
	public void nextTuple() {
		System.err.println("Hay siguiente mensaje: " + consumerIterator.hasNext());
		if(consumerIterator.hasNext()){
			MessageAndMetadata<byte[], byte[]> data = consumerIterator.next();
			String tweet_json = new String(data.message());
			System.err.println(tweet_json);
			//LOG.info(tweet_json);

			// JSON parsing
			ObjectMapper om = new ObjectMapper();
			JsonNode rootNode;

			try {
				rootNode = om.readValue(tweet_json, JsonNode.class);

				String hashtagsList = "";
				if (rootNode.has("created_at") || rootNode.has("\"created_at\"")){
					System.err.println("Inside created_at with value:" + rootNode.get("created_at"));
					for (JsonNode node : rootNode.get("entities").path("hashtags")) {
						System.err.println(hashtagsList);
						hashtagsList = hashtagsList + "#"+ node.get("text").asText();
					}
					String lang = rootNode.get("lang").asText();
	
					String timeStamp = rootNode.get("timestamp_ms").asText();
					System.err.println("Lang is :" +lang);
					for (String validLanguage : this.languagesList) {
						if (validLanguage.contains(lang)) {
							Values value = new Values(timeStamp, lang, hashtagsList);
							System.err.println("---> Storm SPOUT [" + Top3App.SPOUT_ID + "] emiting [" + timeStamp
									+ "- " + lang + " - " + hashtagsList + "]........");
							collector.emit(Top3App.TWITTER_OUTSTREAM, value);
						}
					}
				}
			} catch (IOException | NullPointerException e) {
				System.err.println("---> Storm SPOUT [" + Top3App.SPOUT_ID + "] ERROR: "+e.getMessage());
			}

		}
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
