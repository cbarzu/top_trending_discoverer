package master2015;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;
import kafka.server.KafkaConfig;

public class TwitterHashtagsSpout extends BaseRichSpout {
	private SpoutOutputCollector collector;
	private String zookeeper_url;
	private Set<String> languagesSet;
	private ConsumerIterator consumerIterator;

	public static final String LANG_FIELD = "lang";
	public static final String HASHTAGS_FIELD = "hashtags";
	public static final String KAFKA_TOPIC = "TWITTER_GENERAL3";

	public TwitterHashtagsSpout(String zookeeper_url, Set<String> languages) {
		this.zookeeper_url = zookeeper_url;
		this.languagesSet = languages;
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;

		ConsumerConnector kafkaConsumer = kafka.consumer.Consumer
				.createJavaConsumerConnector(this.createConsumerConfig(this.zookeeper_url, "1"));
		
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(TwitterHashtagsSpout.KAFKA_TOPIC, 1);
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = kafkaConsumer.createMessageStreams(topicCountMap);
		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(TwitterHashtagsSpout.KAFKA_TOPIC);
		consumerIterator = streams.get(0).iterator();
	}

	private String getMessage(Message message) {
		ByteBuffer buffer = message.payload();
		byte[] bytes = new byte[buffer.remaining()];
		buffer.get(bytes);
		return new String(bytes);
	}

	@Override
	public void nextTuple() {
		//if(consumerIterator.hasNext()){
		//	Object o = consumerIterator.next().message();
		//	if(o != null){
		//		System.out.println(o.toString());
		//	}
			//System.out.println("Received: "+new String((String) consumerIterator.next().message()));
		//}
		if(consumerIterator.hasNext()){
			Object m = consumerIterator.next().message();
			System.out.println(m.toString());
		}
		//System.out.println("--------------------------->Hola");
		// GROUP ID????
		//Message msg = (Message) consumerIterator.next().message();
		//System.out.println(msg.toString());
		//System.out.println("Spout receiving : "+this.getMessage(msg));
		/*if (msg != null && msg.isValid()) {
			String tweet_json = this.getMessage(msg);

			ObjectMapper om = new ObjectMapper();
			JsonNode rootNode;

			try {
				rootNode = om.readValue(tweet_json, JsonNode.class);

				String hashtagsList = "";

				for (JsonNode node : rootNode.path("hashtags")) {
					hashtagsList = hashtagsList + node.get("text").toString() + "#";
				}

				String lang = rootNode.get("lang").toString();

				//if (languagesSet.contains(lang)) {
					Values value = new Values(lang, hashtagsList);
					collector.emit(Top3App.TWITTER_OUTSTREAM, value);
				//}
			} catch (IOException e) {
				e.printStackTrace();
			}

		}*/

		// collector.emit(Top3App.TWITTER_OUTSTREAM, new Values("ja","hola"));
		// Blocking method
		/*
		 * while(it.hasNext()){ System.out.println("Received: "+new
		 * String(it.next().message())); }
		 */

		/*
		 * Values randomValue = this.randomValue(); System.out.println(
		 * "emitting "+randomValue);
		 * collector.emit(CurrencySpout.CURRENCYOUTSTREAM, randomValue);
		 */
	}

	/*
	 * private Values randomValue() { double value = Math.random() * 100; return
	 * new Values(AvailableCurrencyUtils.getRandomCurrency(), value); }
	 */

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(Top3App.TWITTER_OUTSTREAM,
				new Fields(TwitterHashtagsSpout.LANG_FIELD, TwitterHashtagsSpout.HASHTAGS_FIELD));

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
