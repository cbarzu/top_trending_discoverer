package master2015;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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

	/** Path of the result file **/
	private String filePath;

	/** Window algorithm size of the window **/
	private long winSize;

	/** Window algorithm advance of the window **/
	private long winAdvance;

	/** Window parameters **/
	private long start_timestamp; // First timestamp received

	/** Current window bounds **/
	private long window_lower_bound;
	private long window_upper_bound;

	/** Current hashtags **/
	private Map<String, Long> hashtagsCount;

	public TwitterHashtagsBolt(String boltId, String path, long winSize, long winAdvance) {
		this.bolt_ID = boltId;
		this.winSize = winSize;
		this.winAdvance = winAdvance;
		this.filePath = path;

		this.start_timestamp = -1;
		this.hashtagsCount = new HashMap<String, Long>();
	}

	@Override
	public void execute(Tuple input) {
		String hashtagsString = (String) input.getValueByField(TwitterHashtagsSpout.HASHTAGS_FIELD);
		String lang = (String) input.getValueByField(TwitterHashtagsSpout.LANG_FIELD);
		long timeStamp = Long
				.parseLong(((String) input.getValueByField(TwitterHashtagsSpout.TIMESTAMP_FIELD)).replaceAll("\"", ""));
		System.out.println("---> Storm BOLT [" + this.bolt_ID + "] receiving [" + timeStamp + " - " + lang + " - "
				+ hashtagsString + "]........");

		/* Configure window (only for the first message received) */
		if (this.start_timestamp == -1) {
			// Set the current window bounds
			long first_timestamp = timeStamp;
			long mult = first_timestamp / winAdvance;
			this.start_timestamp = this.winAdvance * mult;
			this.window_lower_bound = this.start_timestamp;
			this.window_upper_bound = this.window_lower_bound + this.winSize;

			System.out.println("---> Storm BOLT [" + this.bolt_ID + "] setting window parameters [start_time: "
					+ this.start_timestamp + ", window bounds: [" + this.window_lower_bound + " - "
					+ (this.window_upper_bound - 1) + "]........");
		}

		// Get hashtags
		List<String> hashtagList = new ArrayList<String>();
		String[] hashtags = hashtagsString.split("#");
		for (int i = 0; i < hashtags.length; i++) {
			hashtagList.add(hashtags[i]);
		}

		// If the tweet is in the current window
		if (timeStamp >= this.window_lower_bound && timeStamp < this.window_upper_bound) {
			for (String hashtag : hashtagList) {
				// If the hashtag is already in the map
				if (hashtag != null && !hashtag.isEmpty()) {
					if (this.hashtagsCount.containsKey(hashtag)) {
						this.hashtagsCount.put(hashtag, this.hashtagsCount.get(hashtag) + 1);
					} else { // If it's the first arrival of the hashtag, put
								// with
								// freq. 1
						this.hashtagsCount.put(hashtag, new Long(1));
					}
				}
			}
		} else if (timeStamp >= this.window_upper_bound) {
			// Updates the window bounds and generate file result for current
			// bounds
			this.window_lower_bound = this.window_lower_bound + this.winAdvance;
			this.window_upper_bound = this.window_lower_bound + this.winSize;

			List<String> top3 = this.getTop3();

			if (top3 != null) {
				// Write in the file
				File file = new File(this.filePath + "/" + lang + "_12.log");

				try {	    	        
					BufferedWriter writer = new BufferedWriter(new FileWriter(file,true));
					String tops = "";
					for (String top : top3) {
						tops = tops + "," + top;
					}

					writer.write(this.window_lower_bound + "," + lang + tops+"\n");

					writer.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			this.hashtagsCount.clear();

			System.out.println("---> Storm BOLT [" + this.bolt_ID + "] updating window bounds ["
					+ this.window_lower_bound + " - " + (this.window_upper_bound - 1) + "]........");
		}
	}

	@Override
	@SuppressWarnings("rawtypes")
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputDeclarer) {
		outputDeclarer.declareStream(Top3App.TWITTER_OUTSTREAM, new Fields(TwitterHashtagsSpout.LANG_FIELD));
	}

	@SuppressWarnings({ "static-access" })
	List<String> getTop3() {
		List<String> result = new ArrayList<String>();

		if (!this.hashtagsCount.isEmpty()) {
			List<Entry<String, Long>> orderedResults = (List<Entry<String, Long>>) this
					.entriesSortedByValues(this.hashtagsCount);

			for (int i = 0; (i < orderedResults.size() && i < 3); i++) {
				String top = orderedResults.get(i).getKey() + "," + orderedResults.get(i).getValue();
				result.add(top);
			}
			if (orderedResults.size() < 3) { // If there are less than 3
												// different hashtags
				for (int i = 0; i < (3 - orderedResults.size()); i++) {
					result.add("null,0");
				}
			}

			return result;
		} else { // If there are no hashtags
			return null;
		}
	}

	static <K, V extends Comparable<? super V>> List<Entry<K, V>> entriesSortedByValues(Map<K, V> map) {

		List<Entry<K, V>> sortedEntries = new ArrayList<Entry<K, V>>(map.entrySet());

		Collections.sort(sortedEntries, new Comparator<Entry<K, V>>() {
			@Override
			public int compare(Entry<K, V> e1, Entry<K, V> e2) {
				return e2.getValue().compareTo(e1.getValue());
			}
		});

		return sortedEntries;
	}
}
