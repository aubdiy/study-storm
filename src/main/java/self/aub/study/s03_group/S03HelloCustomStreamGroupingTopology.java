package self.aub.study.s03_group;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

/**
 * @author liujinxin
 * @since 2015-07-10 17:17
 */
public class S03HelloCustomStreamGroupingTopology {
    public static void main(String[] args) {


        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("citys", new S03HelloGroupSpout());
        builder.setBolt("hello", new S03HelloGroupBolt(), 2).customGrouping("citys", new S03HelloCustomStreamGrouping());//自定义分组

        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxSpoutPending(2);


        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());
    }
}
