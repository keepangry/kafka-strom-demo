/*
 * @(#)WordCountTopology.java        1.0 2018年3月14日
 *
 *
 */

package cn.kuangxf.example.word;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import cn.kuangxf.example.word.bolt.SplitSentenceBolt;
import cn.kuangxf.example.word.bolt.WordCountBolt;
import cn.kuangxf.example.word.spout.SentenceSpout;

/**
 * Class description goes here.
 *
 * @version 1.0 2018年3月14日
 * @author Administrator
 * @history
 * 
 */
public class WordCountTopology {
	public static void main(String[] args) throws Exception {

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new SentenceSpout(), 1);
		builder.setBolt("split", new SplitSentenceBolt(), 10).shuffleGrouping("spout");
		builder.setBolt("count", new WordCountBolt(), 10).fieldsGrouping("split", new Fields("word"));

		Config conf = new Config();
		conf.setDebug(false);

		if (args != null && args.length > 0) {
			// 集群模式
			conf.setNumWorkers(4);
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		} else {
			// 本地模式
			LocalCluster cluster = new LocalCluster();
			conf.setDebug(true);
			cluster.submitTopology("word-count", conf, builder.createTopology());
			Thread.sleep(10000);
			cluster.shutdown();
		}
	}
}
