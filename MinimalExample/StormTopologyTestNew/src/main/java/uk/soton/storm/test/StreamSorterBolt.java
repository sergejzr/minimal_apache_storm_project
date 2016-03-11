package uk.soton.storm.test;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * This bolt performs tokenization - splitting incoming tweets into words
 * (tokens)
 * 
 * @author Zerr
 * 
 */
public class StreamSorterBolt extends BaseRichBolt {
	private OutputCollector collector;

	private static final long serialVersionUID = 4896669174997566924L;

	public String getStreamId() {
		return "MyStream:" + this.getClass();
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;

	}

	@Override
	public void execute(Tuple tuple) {

		Integer randomdata = (Integer) tuple.getValueByField("MyData");

		collector.emit(getStreamId(), new Values(randomdata, 45));

		collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(getStreamId(), new Fields("MyData1", "MyData2"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return super.getComponentConfiguration();
	}

	@Override
	public void cleanup() {
		super.cleanup();
	}

}
