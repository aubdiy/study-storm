package self.aub.study.s05_trident.state.opaque;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import self.aub.study.s05_trident.S05PrintFilter;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;

/**
 * @author liujinxin
 * @since 2015-07-17 16:36
 */
public class S05OpaqueTransactionalTotology {


    public static void main(String[] args) {
        TridentTopology topology = new TridentTopology();
        //TridentState tridentState = topology.newStaticState(new S05BasicStateFactory());
        topology.newStream("test", new S05OpaqueTransactionalSpout()).parallelismHint(2)
//                .shuffle()
//                .stateQuery(tridentState, new Fields("tx","city"), new S05BasicQuery(), new Fields("population")).parallelismHint(1)
//                .each(new Fields("tx", "city", "population", "index"), new S05PrintFilter())
//                .partitionPersist(new S05BasicStateFactory(), new Fields("city"), new S05BasicUpdate(), new Fields("city", "population"))
//
//                .newValuesStream()
//                .shuffle()
//                .each(new Fields("city", "population"), new S05PrintFilter())

                .groupBy(new Fields("tx","city"))
                .persistentAggregate(new S05OpaqueTransactionlStateFactory(), new Count(), new Fields("count")).parallelismHint(3)
                .newValuesStream()
                .shuffle()
                .each(new Fields("city", "count"), new S05PrintFilter())
        ;


        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxSpoutPending(1);
        conf.setMessageTimeoutSecs(5000);


        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, topology.build());
    }
}
