package master;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;



public class TwitterApp {
	
	public static final String KAFKA_TOPIC = "TWITTER_GENERAL3";
	public static void main(String[] args) {
		
		if(args.length!=7){
			System.err.println("Error: wrong arguments");
			System.exit(-1);
		}
		
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,args[5]);
		props.put(ProducerConfig.RETRIES_CONFIG, "3");
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 200);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
		
		int mode = 0;
		try{
			mode = Integer.parseInt(args[0]);
		} catch(NumberFormatException e){
			System.err.println("Wrong mode argument");
			System.exit(-2);
		}
		
		switch(mode){
		case 1:
			new TweetReaderFile(producer,args[6]);
			break;
		case 2:
			new TweetReaderAPI(producer,args[1],args[2],args[3],args[4]);
			break;
		}
		
	}
}
