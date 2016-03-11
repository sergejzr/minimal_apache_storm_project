package uk.soton.storm.test;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;


/**
 * This spout emmits two streams, a configuration stream and tweet stream.
 * 
 * @author Alrifai, Zerr
 * 
 */
public class SpoutSomedata extends BaseRichSpout {
	private static final long serialVersionUID = 3188867013620053529L;
	private SpoutOutputCollector collector = null;


	/**
	 * Creates A spout, which emmits two streams. One "cofigurationStream" (only
	 * one emmit at the beginning with ITwitterStreamData2Output object) and
	 * "symbolsStream" (with ITwitterStreamData1Output)
	 * 
	 * @param dataPath
	 *            a path to a local file directory, or HDFS path
	 * @param local
	 *            if true - will try to run on local files
	 * @param adjusttonow
	 *            - timestamps of the tweets are adjusted to NOW as they would
	 *            have been send at this moment
	 * @param speedfactor
	 *            between 0.0 and MAX_DOUBLE. Example: 0 - max speed(no delay),
	 *            0.5 - two times faster than original, 1.0 - original speed,
	 *            2.0 - two times slower.
	 * @param readLopps
	 *            how many times it should restart after all files are done
	 */
	public SpoutSomedata() {
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;

	
	}

	public String getStreamId() {
		return this.getClass() + "_spoutstream";
	}

	@Override
	public void nextTuple() {

		Random r = new Random();
		try {
			Thread.sleep(r.nextInt(5000));
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		collector.emit(getStreamId(), new Values(r.nextInt(1000)));

	}

	@Override
	public void ack(Object msgId) {
		super.ack(msgId);
	}

	@Override
	public void fail(Object msgId) {
		super.fail(msgId);
	}

	@Override
	public void close() {
		super.close();
		try {
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return super.getComponentConfiguration();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		declarer.declareStream(getStreamId(), new Fields("MyData"));

	}



}
