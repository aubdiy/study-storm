package self.aub.study.s03_group;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

/**
 * @author liujinxin
 * @since 2015-07-10 17:17
 */
public class S03HelloGroupTopology {
    public static void main(String[] args) {


        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("citys", new S03HelloGroupSpout());
        builder.setBolt("hello", new S03HelloGroupBolt()).shuffleGrouping("citys");//随机分组
//        builder.setBolt("hello", new S03HelloGroupBolt(), 2).fieldsGrouping("citys", new Fields("city"));//字段分组
//        builder.setBolt("hello", new S03HelloGroupBolt(), 2).allGrouping("citys");//全数据分组
//        builder.setBolt("hello", new S03HelloGroupBolt(), 2).globalGrouping("citys");//全局分组
//        builder.setBolt("hello", new S03HelloGroupBolt(), 2).noneGrouping("citys");//不分组（目前同随机分组）
//        builder.setBolt("hello", new S03HelloGroupBolt(), 2).localOrShuffleGrouping("citys");//本地或随机分组


        //控制并发度·
        StormTopology topology = builder.createTopology();
        topology.get_bolts().get("hello").get_common().set_parallelism_hint(3);


        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxSpoutPending(2);


        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, topology);
    }
}
