package self.aub.study.s02_basic;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import self.aub.study.s01_rich.S01HelloRichBolt;
import self.aub.study.s01_rich.S01HelloRichSpout;

/**
 * @author liujinxin
 * @since 2015-07-10 17:17
 */
public class S02HelloBasicTopology {
    public static void main(String[] args) {


        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("msgs", new S02HelloSpout());
        builder.setBolt("hello", new S02HelloBasicBolt()).shuffleGrouping("msgs");

        Config conf = new Config();
        conf.setMaxSpoutPending(2);


        LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", conf, builder.createTopology());
    }
}
