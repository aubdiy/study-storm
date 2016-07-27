package base;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

import java.util.ArrayList;
import java.util.UUID;


/**
 * @author liujinxin
 * @since 2015-06-29 16:33
 */
public class TTBase {
    public static void main(String[] args) {
        String zkConnString = "127.0.0.1:2181";
        String brokerZkPath="/kafka_2.10-0.8.2.1-cluster/brokers";
        String topicName = "tt";

        BrokerHosts hosts = new ZkHosts(zkConnString,brokerZkPath);
        SpoutConfig spoutConfig = new SpoutConfig(hosts, topicName, "/test-storm-kafka/" + topicName, "aa");
        spoutConfig.scheme = new SchemeAsMultiScheme(new TTScheme());
        spoutConfig.startOffsetTime=kafka.api.OffsetRequest.EarliestTime();

        spoutConfig.zkServers = new ArrayList<String>();
        spoutConfig.zkServers.add("127.0.0.1");
        spoutConfig.zkPort=2181;

        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", kafkaSpout);
        builder.setBolt("bolt", new TTBolt(), 1).globalGrouping("spout");

        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(2);
        conf.setMaxSpoutPending(100);
        // StormSubmitter.submitTopology("KafKaTopology", conf,
        // builder.createTopology());

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());

    }
}
