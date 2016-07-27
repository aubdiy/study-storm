package self.aub.study.s04_transaction.partition;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.transactional.TransactionalTopologyBuilder;
import backtype.storm.tuple.Fields;
import self.aub.study.s04_transaction.S04HelloCommiterBolt;
import self.aub.study.s04_transaction.S04HelloCountBolt;
import self.aub.study.s04_transaction.S04HelloSplitBolt;
import self.aub.study.s04_transaction.S04HelloTransactionalSpout;

/**
 * @author liujinxin
 * @since 2015-07-10 17:17
 */
public class S04HelloPartitionedTopology {
    public static void main(String[] args) {



        TransactionalTopologyBuilder builder= new TransactionalTopologyBuilder("test", "spout", new S04HelloPartitionedSpout(),2);

        builder.setBolt("bolt-split", new S04HelloSplitBolt(), 2).shuffleGrouping("spout");
        builder.setBolt("bolt-count", new S04HelloCountBolt(), 2).fieldsGrouping("bolt-split", new Fields("word"));
        builder.setCommitterBolt("bolt-commiter", new S04HelloCommiterBolt()).globalGrouping("bolt-count");


        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxSpoutPending(2);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.buildTopology());
    }
}
