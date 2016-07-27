package self.aub.study.s03_group;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

/**
 * @author liujinxin
 * @since 2015-07-10 17:17
 */
public class S03HelloDirectGroupTopology {
    public static void main(String[] args) {


        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("citys", new S03HelloDirectGroupSpout());
        builder.setBolt("hello", new S03HelloGroupBolt(), 2).directGrouping("citys");//直接分组

        Config conf = new Config();
        conf.setMaxSpoutPending(2);


        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());
    }
}
