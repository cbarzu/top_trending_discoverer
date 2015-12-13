package master2015;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class TwitterHashtagsBolt extends BaseRichBolt{

	@Override
	public void execute(Tuple input) {
		/*Currency currencyID = (Currency) input.getValueByField(CurrencySpout.CURRENCYFIELDNAME);
		double valueField = (Double) input.getValueByField(CurrencySpout.CURRENCYFIELDVALUE);
		double rate = AvailableCurrencyUtils.getRate(currencyID);
		double euroValue = valueField * rate;
		System.out.println("EUR: "+euroValue+", "+currencyID+": "+valueField);*/
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		
	}

}
