package self.aub.study.s05_trident.state.partitioned;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import self.aub.study.s05_trident.S05PrintFilter;
import self.aub.study.s05_trident.state.non.S05BasicStateFactory;
import self.aub.study.s05_trident.state.non.S05BasicUpdate;
import storm.trident.TridentTopology;

/**
 * @author liujinxin
 * @since 2015-07-17 16:36
 */
public class S05PartitionedTransactionalTotology {


    public static void main(String[] args) {
        TridentTopology topology = new TridentTopology();
        //TridentState tridentState = topology.newStaticState(new S05BasicStateFactory());
        topology.newStream("test", new S05PartitionedTransactionalSpout()).parallelismHint(2)

                //.stateQuery(tridentState, new Fields("city"), new S05BasicQuery(), new Fields("population"))
                //.each(new Fields("batch_id", "city", "population", "index"), new S05PrintFilter())
                .partitionPersist(new S05BasicStateFactory(), new Fields("city"), new S05BasicUpdate(), new Fields("city", "population"))

                .newValuesStream()
                .shuffle()
                .each(new Fields("city","population"), new S05PrintFilter())
        ;


        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxSpoutPending(1);
        conf.setMessageTimeoutSecs(5);


        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, topology.build());
    }
}
