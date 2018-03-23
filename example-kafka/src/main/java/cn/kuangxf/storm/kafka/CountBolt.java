package cn.kuangxf.storm.kafka;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class CountBolt implements IRichBolt {

	 Map<String, Integer> counters;
	   private OutputCollector collector;

	   @Override
	   public void prepare(Map stormConf, TopologyContext context,
	   OutputCollector collector) {
	      this.counters = new HashMap<String, Integer>();
	      this.collector = collector;
	   }

	   @Override
	   public void execute(Tuple input) {
	      String str = input.getString(0);
	      System.out.println("---------->单词：" + str) ;
	      if(!counters.containsKey(str)){
	         counters.put(str, 1);
	      }else {
	         Integer c = counters.get(str) +1;
	         counters.put(str, c);
	      }

	      collector.ack(input);
	   }

	   @Override
	   public void cleanup() {
		   System.out.println("----------统计结果--------------");
	      for(Map.Entry<String, Integer> entry:counters.entrySet()){
	         System.out.println(entry.getKey()+" : " + entry.getValue());
	      }
	   }

	   @Override
	   public void declareOutputFields(OutputFieldsDeclarer declarer) {

	   }

	   @Override
	   public Map<String, Object> getComponentConfiguration() {
	      return null;
	   }

}
