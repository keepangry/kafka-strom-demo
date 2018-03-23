/*
 * @(#)SplitSentenceBolt.java        1.0 2018年3月14日
 *
 *
 */

package cn.kuangxf.example.word.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * Class description goes here.
 *
 * @version 1.0 2018年3月14日
 * @author Administrator
 * @history
 * 
 */
public class SplitSentenceBolt extends BaseBasicBolt {
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// 定义了传到下一个bolt的字段描述
		declarer.declare(new Fields("word"));
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String sentence = input.getStringByField("sentence");
		String[] words = sentence.split(" ");
		for (String word : words) {
			// 发送单词
			collector.emit(new Values(word));
		}
	}

}
