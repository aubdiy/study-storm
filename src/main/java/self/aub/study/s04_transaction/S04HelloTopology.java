package self.aub.study.s04_transaction;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.transactional.TransactionalTopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * @author liujinxin
 * @since 2015-07-10 17:17
 */
public class S04HelloTopology {
    public static void main(String[] args) {

        TransactionalTopologyBuilder builder= new TransactionalTopologyBuilder("test", "spout", new S04HelloTransactionalSpout());


        builder.setBolt("bolt-split", new S04HelloSplitBolt(), 2).shuffleGrouping("spout");
        builder.setBolt("bolt-count", new S04HelloCountBolt(), 1).fieldsGrouping("bolt-split", new Fields("word"));
        builder.setCommitterBolt("bolt-commiter", new S04HelloCommiterBolt()).globalGrouping("bolt-count");
//        builder.setCommitterBolt("bolt-commiter",new S04HelloCommiterBolt(),2 ).shuffleGrouping("bolt-count");


        StormTopology topology = builder.buildTopology();

        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxSpoutPending(2);


        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, topology);
    }
}
