package master2015;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class TwitterHashtagsBolt extends BaseRichBolt{

	//private BufferedWriter writer;
	
	public TwitterHashtagsBolt(long boltId, String path) {
		//File file = new File(path+"/result"+boltId+".txt");
		
		/*try {
			BufferedWriter writer = new BufferedWriter(new FileWriter(file));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
	}
	
	@Override
	public void execute(Tuple input) {
		String hashtagsList = (String)input.getValueByField(TwitterHashtagsSpout.HASHTAGS_FIELD);
		System.out.println("------------------------------------> "+input.toString());
		String[] hashtags = hashtagsList.split("#");
		/*for(String ht : hashtags){
			try {
				this.writer.write(ht);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			System.out.println("------------------------------------> "+ht);
		}*/
		/*Currency currencyID = (Currency) input.getValueByField(CurrencySpout.CURRENCYFIELDNAME);
		double valueField = (Double) input.getValueByField(CurrencySpout.CURRENCYFIELDVALUE);
		double rate = AvailableCurrencyUtils.getRate(currencyID);
		double euroValue = valueField * rate;
		System.out.println("EUR: "+euroValue+", "+currencyID+": "+valueField);*/
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		
	}

}
