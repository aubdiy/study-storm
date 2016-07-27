package self.aub.study.s01_rich.stream;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import self.aub.study.s01_rich.S01HelloRichSpout;

/**
 * @author liujinxin
 * @since 2015-07-10 17:17
 */
public class S01HelloStreamTopology {
    public static void main(String[] args) {


        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("citys", new S01HelloRichSpout());


//        builder.setBolt("assembly-bolt-a", new S01HelloStreamBoltA1()).shuffleGrouping("citys");
        builder.setBolt("assembly-bolt-b", new S01HelloStreamBoltB()).shuffleGrouping("citys");
        builder.setBolt("assembly-bolt-c", new S01HelloStreamBoltC())
//                .shuffleGrouping("assembly-bolt-a1")
                .shuffleGrouping("assembly-bolt-b", "stream-B1");
        builder.setBolt("assembly-bolt-d", new S01HelloStreamBoltD()).shuffleGrouping("assembly-bolt-b", "stream-B2");
        builder.setBolt("assembly-bolt-e", new S01HelloStreamBoltD()).shuffleGrouping("assembly-bolt-b");
        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxSpoutPending(2);


        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());
    }
}
