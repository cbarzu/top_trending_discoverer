package master2015;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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

public class TwitterHashtagsSpout extends BaseRichSpout {
	private SpoutOutputCollector collector;
	private String zookeeper_url;
	
	public static final String TWEET_FIELD_NAME = "TweetName";
	public static final String TWEET_FIELD_VALUE = "TweetValue";
	public static final String KAFKA_TOPIC = "Hashtags Topic";

	public TwitterHashtagsSpout(String zookeeper_url) {
		this.zookeeper_url=zookeeper_url;
	}
	
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;

	}

	@Override
	public void nextTuple() {
		//GROUP ID????
		ConsumerConnector kafkaConsumer = Consumer
				.createJavaConsumerConnector(this.createConsumerConfig(this.zookeeper_url, "1"));

		Map<String, Integer> topicCountMap = new HashMap<>();
		topicCountMap.put(TwitterHashtagsSpout.KAFKA_TOPIC, 1);
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = kafkaConsumer.createMessageStreams(topicCountMap);

		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(TwitterHashtagsSpout.KAFKA_TOPIC);

		ConsumerIterator<byte[], byte[]> it = streams.get(0).iterator();

		while (it.hasNext()) {
			String s = new String(it.next().message());
			String[] fields = s.split("#");
			Values value = new Values(fields[0], fields[1]);
			collector.emit(Top3App.TWITTER_OUTSTREAM, value);
		}

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

	/*private Values randomValue() {
		double value = Math.random() * 100;
		return new Values(AvailableCurrencyUtils.getRandomCurrency(), value);
	}*/

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(Top3App.TWITTER_OUTSTREAM,
				new Fields(TwitterHashtagsSpout.TWEET_FIELD_NAME, TwitterHashtagsSpout.TWEET_FIELD_VALUE));

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
