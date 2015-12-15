package master2015;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/**
 * 
 * TwitterHashtagsBolt: Storm Bolt
 * 
 * Receives events from Spouts and process the information
 * 
 * @author Claudiu Barzu (claudiu.barzu@alumnos.upm.es)
 * @author Javier Villar Gil (javier.villar.gil@alumnos.upm.es)
 *
 */
@SuppressWarnings("serial")
public class TwitterHashtagsBolt extends BaseRichBolt {
	/** Storm Bolt ID **/
	private String bolt_ID;
	
	/** Window algorithm size of the window **/
	private long winSize;
	
	/** Window algorithm advance of the window **/
	private long winAdvance;
	
	/** Window parameters **/
	private long start_timestamp;		//First timestamp received 
	
	//Current window bounds
	private long window_lower_bound;
	private long window_upper_bound;
	
	// private BufferedWriter writer;

	public TwitterHashtagsBolt(String boltId, String path, long winSize, long winAdvance) {
		this.bolt_ID = boltId;
		this.winSize = winSize;
		this.winAdvance = winAdvance;
		
		this.start_timestamp = -1;
		
		// File file = new File(path + "/result" + boltId + ".txt");
		//
		// try {
		// BufferedWriter writer = new BufferedWriter(new FileWriter(file));
		// } catch (IOException e) {
		// e.printStackTrace();
		// }

	}

	@Override
	public void execute(Tuple input) {
		String hashtagsList = (String) input.getValueByField(TwitterHashtagsSpout.HASHTAGS_FIELD);
		String lang = (String) input.getValueByField(TwitterHashtagsSpout.LANG_FIELD);
		long timeStamp = Long.parseLong(((String) input.getValueByField(TwitterHashtagsSpout.TIMESTAMP_FIELD)).replaceAll("\"", ""));
		//System.out.println("---> Storm BOLT [" + this.bolt_ID + "] receiving ["+timeStamp +" - "+lang +" - "+hashtagsList+"]........");

		/* Configure window (only for the first message received) */
		if(this.start_timestamp == -1){
			//Set the current window bounds
			long first_timestamp = timeStamp;
			long mult = first_timestamp / winAdvance;
			this.start_timestamp = this.winAdvance * mult;
			this.window_lower_bound = this.start_timestamp;
			this.window_upper_bound = this.window_lower_bound + this.winSize;
			
			System.out.println("---> Storm BOLT [" + this.bolt_ID + "] setting window parameters [start_time: "+this.start_timestamp +", window bounds: ["+this.window_lower_bound +" - "+(this.window_upper_bound-1)+"]........");
		}
		
		
		if(timeStamp >= this.window_lower_bound && this.start_timestamp < this.window_upper_bound){
			
		} else if (timeStamp >= this.window_upper_bound){	//Update window bounds and generate file result for current bounds
			//TODO write in file
			
			this.window_lower_bound = this.window_lower_bound+this.winAdvance;
			this.window_upper_bound = this.window_lower_bound+this.winSize;
			
			System.out.println("---> Storm BOLT [" + this.bolt_ID + "] updating window bounds ["+this.window_lower_bound +" - "+(this.window_upper_bound-1)+"]........");
		}
		
		
		/** TODO Extraer hashtags */
		/** TODO Ventana y top 3 */
		/** TODO Escritura en fichero */
		// String[] hashtags = hashtagsList.split("#");

		// for(String ht : hashtags){
		// try {
		// this.writer.write(ht);
		// } catch (IOException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		//
		// System.out.println("------------------------------------> "+ht);
		// }

		// Currency currencyID = (Currency)
		// input.getValueByField(CurrencySpout.CURRENCYFIELDNAME);
		// double valueField = (Double)
		// input.getValueByField(CurrencySpout.CURRENCYFIELDVALUE);
		// double rate = AvailableCurrencyUtils.getRate(currencyID);
		// double euroValue = valueField * rate;
		// System.out.println("EUR: "+euroValue+", "+currencyID+":
		// "+valueField);
	}

	@Override
	@SuppressWarnings("rawtypes")
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputDeclarer) {
		outputDeclarer.declareStream(Top3App.TWITTER_OUTSTREAM, new Fields(TwitterHashtagsSpout.LANG_FIELD));
	}

}
