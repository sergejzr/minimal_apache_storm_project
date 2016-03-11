package uk.soton.storm.test;

import java.io.IOException;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

/**
 * Runs a test subtopology on a local cluster. This is an example how to use the
 * subtopology and tweet file simulation libraries
 * 
 * @author Zerr
 * 
 */
public class TestFramework {

	public void runTopology() throws IOException {

		StreamSorterBolt streamSorterBolt = new StreamSorterBolt();
		StreamAnalyzerBolt streamAnalyzerBolt = new StreamAnalyzerBolt();
		StreamVizualizerBolt streamVizualizerBolt = new StreamVizualizerBolt();

		TopologyBuilder builder = new TopologyBuilder();

		Config config = new Config();
		SpoutSomedata input = new SpoutSomedata();

		String prefix;
		Number maxTaskParallelism = 1;

		builder.setSpout("MyCrowdSpout", input);

		builder.setBolt(streamSorterBolt.getClass() + "_bolt", streamSorterBolt, maxTaskParallelism)
				.shuffleGrouping("MyCrowdSpout", input.getStreamId());

		builder.setBolt(streamAnalyzerBolt.getClass() + "_bolt", streamAnalyzerBolt, 2)
		.shuffleGrouping(streamSorterBolt.getClass() + "_bolt",
				streamSorterBolt.getStreamId());

		builder.setBolt(streamVizualizerBolt.getClass() + "_bolt", streamVizualizerBolt, 1)
		.shuffleGrouping(streamAnalyzerBolt.getClass() + "_bolt",
				streamAnalyzerBolt.getStreamId());
		
		

		// config.setDebug(true);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("MyTestTopo", config, builder.createTopology());

		System.in.read();
		cluster.killTopology("MyTestTopo");
		cluster.shutdown();
	}

	public static void main(String[] args) throws Exception {

		TestFramework tf = new TestFramework();

		tf.runTopology();

	}
}
