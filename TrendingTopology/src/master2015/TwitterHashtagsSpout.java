package master2015;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

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

public class TwitterHashtagsSpout extends BaseRichSpout {
	private SpoutOutputCollector collector;
	private String zookeeper_url;
	private List<String> languagesList;
	private ConsumerIterator consumerIterator;

	private String[] tweets = new String[]{"{\"created_at\":\"Sun Dec 13 15:05:06 +0000 2015\",\"id\":676055249598070784,\"id_str\":\"676055249598070784\",\"text\":\"\\u3059\\u304d\\u306e\\u610f\\u5473\\u3001\\u308f\\u304b\\u3089\\u3093\",\"source\":\"\\u003ca href=\\\"http:\\/\\/twitter.com\\/download\\/iphone\\\" rel=\\\"nofollow\\\"\\u003eTwitter for iPhone\\u003c\\/a\\u003e\",\"truncated\":false,\"in_reply_to_status_id\":null,\"in_reply_to_status_id_str\":null,\"in_reply_to_user_id\":null,\"in_reply_to_user_id_str\":null,\"in_reply_to_screen_name\":null,\"user\":{\"id\":3101529151,\"id_str\":\"3101529151\",\"name\":\"\\u3048\\u3068\\u308f\\u308b\\u3055\\u3093\\u3002\",\"screen_name\":\"etowaru__\",\"location\":null,\"url\":null,\"description\":\"\\u30b3\\u30df\\u30e5\\u969c\\u3060\\u3051\\u3069Twitter\\u3055\\u3048\\u3042\\u308c\\u3070\\u95a2\\u4fc2\\u306a\\u3044\\u3088\\u306d\\u3063\\u30fc\\u30fc\\u30fc\\u30fc\\u30fc\\u30fc\\u30fc\\u30fc\\u30fc\\u30fc\\u30fc\\u30fc\\u30fc\\u30fc\\u30fc\\u30fc\\u30fc\\u30fc\\u30fc\\u30fc\\u30fc\\u30fc\\u30fc\\u30fc\\u30fc\\u30fc\\u30fc\\u30fc\\u30fc\\u30fc\\u30fc\\u30fc\\u30fc\\u30fc\\u30fc\\u30fc\\u30fc\\u30fc\\u30fc\\u30fc\\u30fc\\u30fc\\u30fc\\u30fc\\u30fc\\u30fc\\u30fc\\u30fc\\uff01\\uff01\\uff01\\uff01\\uff01\\uff01\\uff01\\uff01\\uff01\\uff01\\uff01\\uff01\\uff01\\uff01\\uff01\\uff01\\uff01\\uff01\\uff01\\uff01\\uff01\\uff01\\uff01\\uff01\\uff01\\uff01\\uff01\\uff57\\uff57\\uff57\\uff57\\uff57\\uff57\\uff57\\uff57\\uff57\\uff57\\uff57\\uff57\\uff57\\uff57\\uff57\\uff57\\uff57\\uff57\\uff57\\uff57\\uff57\\uff57\\uff57\\uff57\\uff57\\uff57\\uff57\",\"protected\":false,\"verified\":false,\"followers_count\":466,\"friends_count\":199,\"listed_count\":17,\"favourites_count\":5050,\"statuses_count\":2051,\"created_at\":\"Sat Mar 21 11:37:16 +0000 2015\",\"utc_offset\":null,\"time_zone\":null,\"geo_enabled\":false,\"lang\":\"ja\",\"contributors_enabled\":false,\"is_translator\":false,\"profile_background_color\":\"C0DEED\",\"profile_background_image_url\":\"http:\\/\\/abs.twimg.com\\/images\\/themes\\/theme1\\/bg.png\",\"profile_background_image_url_https\":\"https:\\/\\/abs.twimg.com\\/images\\/themes\\/theme1\\/bg.png\",\"profile_background_tile\":false,\"profile_link_color\":\"0084B4\",\"profile_sidebar_border_color\":\"C0DEED\",\"profile_sidebar_fill_color\":\"DDEEF6\",\"profile_text_color\":\"333333\",\"profile_use_background_image\":true,\"profile_image_url\":\"http:\\/\\/pbs.twimg.com\\/profile_images\\/673977142045106176\\/Cq5Js3hn_normal.jpg\",\"profile_image_url_https\":\"https:\\/\\/pbs.twimg.com\\/profile_images\\/673977142045106176\\/Cq5Js3hn_normal.jpg\",\"profile_banner_url\":\"https:\\/\\/pbs.twimg.com\\/profile_banners\\/3101529151\\/1449842690\",\"default_profile\":true,\"default_profile_image\":false,\"following\":null,\"follow_request_sent\":null,\"notifications\":null},\"geo\":null,\"coordinates\":null,\"place\":null,\"contributors\":null,\"is_quote_status\":false,\"retweet_count\":0,\"favorite_count\":0,\"entities\":{\"hashtags\":[],\"urls\":[],\"user_mentions\":[],\"symbols\":[]},\"favorited\":false,\"retweeted\":false,\"filter_level\":\"low\",\"lang\":\"ja\",\"timestamp_ms\":\"1450019106660\"}"};
			
	public static final String LANG_FIELD = "lang";
	public static final String HASHTAGS_FIELD = "hashtags";
	public static final String KAFKA_TOPIC = "TWITTER_GENERAL3";

	public TwitterHashtagsSpout(String zookeeper_url, List<String> languages) {
		this.zookeeper_url = zookeeper_url;
		this.languagesList = languages;
	}
	
	
	private String getRandomTweet(){
		int index = new Random().nextInt(1);
		return this.tweets[index];
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;

		/*ConsumerConnector kafkaConsumer = kafka.consumer.Consumer
				.createJavaConsumerConnector(this.createConsumerConfig(this.zookeeper_url, "1"));
		
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(TwitterHashtagsSpout.KAFKA_TOPIC, 1);
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = kafkaConsumer.createMessageStreams(topicCountMap);
		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(TwitterHashtagsSpout.KAFKA_TOPIC);
		consumerIterator = streams.get(0).iterator();*/
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
		/*if(consumerIterator.hasNext()){
			Object m = consumerIterator.next().message();
			System.out.println(m.toString());
		}*/

		
		// GROUP ID????
		//Message msg = (Message) consumerIterator.next().message();
		//System.out.println(msg.toString());
		//System.out.println("Spout receiving : "+this.getMessage(msg));
		String tweet_json = this.getRandomTweet();
		//System.out.println("Spout receiving : "+tweet_json);
		
		if (tweet_json != null && !tweet_json.isEmpty()) {	//tweet.isValid()
			//String tweet_json = this.getMessage(msg);

			ObjectMapper om = new ObjectMapper();
			JsonNode rootNode;

			try {
				rootNode = om.readValue(tweet_json, JsonNode.class);

				String hashtagsList = "";

				for (JsonNode node : rootNode.path("hashtags")) {
					hashtagsList = hashtagsList + node.get("text").toString() + "#";
				}

				String lang = rootNode.get("lang").toString();

				for(String validLanguage : this.languagesList){
					if(validLanguage.equals(lang)){
						Values value = new Values(lang, hashtagsList);
						collector.emit(Top3App.TWITTER_OUTSTREAM, value);
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}

		}

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
