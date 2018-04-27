package cn.kuangxf.storm.kafka;

import java.util.UUID;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) throws Exception{
	      Config config = new Config();
	      config.setDebug(false);
	      config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
	      String zkConnString = "127.0.0.1:2181";
	      String topic = "kafkatopic";
	      BrokerHosts hosts = new ZkHosts(zkConnString);

	      SpoutConfig kafkaSpoutConfig = new SpoutConfig (hosts, topic, "/" + topic,    
	         UUID.randomUUID().toString());
	      kafkaSpoutConfig.bufferSizeBytes = 1024;
	      kafkaSpoutConfig.fetchSizeBytes = 1024;
	      kafkaSpoutConfig.stateUpdateIntervalMs = 2000;
	      //kafkaSpoutConfig.forceFromStart = true;
	      kafkaSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

	      TopologyBuilder builder = new TopologyBuilder();
	      builder.setSpout("kafka-spout", new KafkaSpout(kafkaSpoutConfig));
	      builder.setBolt("word-spitter", new SplitBolt()).shuffleGrouping("kafka-spout");
	      builder.setBolt("word-counter", new CountBolt()).shuffleGrouping("word-spitter");

	      
//	      StormSubmitter.submitTopology(args[0], config, builder.createTopology());
	      LocalCluster cluster = new LocalCluster();
	      cluster.submitTopology("KafkaStormSample", config, builder.createTopology());
	      Thread.sleep(10000);

	      //cluster.shutdown();
	   }
}
