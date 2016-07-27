package self.aub.study.s01_rich.reliably;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

/**
 * @author liujinxin
 * @since 2015-07-10 17:17
 */
public class S01HelloReliablyTopology {
    public static void main(String[] args) {


        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("msgs", new S01HelloReliablySpout());
        builder.setBolt("hello", new S01HelloReliablyBolt()).shuffleGrouping("msgs");

        Config conf = new Config();
        conf.setMaxSpoutPending(2);


        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());
    }
}
