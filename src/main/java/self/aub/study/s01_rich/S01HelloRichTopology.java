package self.aub.study.s01_rich;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

/**
 * @author liujinxin
 * @since 2015-07-10 17:17
 */
public class S01HelloRichTopology {
    public static void main(String[] args) {


        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("citys", new S01HelloRichSpout());
        builder.setBolt("hello", new S01HelloRichBolt()).shuffleGrouping("citys");

        Config conf = new Config();


        LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", conf, builder.createTopology());
    }
}
