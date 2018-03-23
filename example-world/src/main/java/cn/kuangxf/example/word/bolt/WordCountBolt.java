/*
 * @(#)WordCountBolt.java        1.0 2018年3月14日
 *
 *
 */

package cn.kuangxf.example.word.bolt;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

/**
 * Class description goes here.
 *
 * @version 1.0 2018年3月14日
 * @author Administrator
 * @history
 * 
 */
public class WordCountBolt extends BaseBasicBolt {

	private Map<String, Long> counts = null;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		this.counts = new HashMap<String, Long>();
	}

	@Override
	public void cleanup() {
		// 拓扑结束执行
		for (String key : counts.keySet()) {
			System.out.println(key + " : " + this.counts.get(key));
		}
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String word = input.getStringByField("word");
		Long count = this.counts.get(word);
		if (count == null) {
			count = 0L;
		}
		count++;
		this.counts.put(word, count);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

}
