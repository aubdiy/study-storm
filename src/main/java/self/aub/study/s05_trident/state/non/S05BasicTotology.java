package self.aub.study.s05_trident.state.non;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import storm.trident.TridentState;
import storm.trident.TridentTopology;

/**
 * @author liujinxin
 * @since 2015-07-17 16:36
 */
public class S05BasicTotology {


    public static void main(String[] args) {
        TridentTopology topology = new TridentTopology();
        TridentState tridentState = topology.newStaticState(new S05BasicStateFactory());

        topology.newStream("test", new S05NonTransactionalSpout())

                .stateQuery(tridentState, new Fields("city"), new S05BasicQuery(), new Fields("population"))
//                .each(new Fields("batch_id", "city", "population", "index"), new S05PrintFilter())
//                .partitionPersist(new S05BasicStateFactory(), new Fields("city"), new S05BasicUpdate(), new Fields("city", "population"))
//                .newValuesStream()
//                .shuffle()
//                .each(new Fields("population"), new S05PrintFilter())


                //.groupBy(new Fields("city"))
                //.persistentAggregate(new S05NonTransactionlStateFactory(), new Fields("city"), new Count(), new Fields("count"))
                //.newValuesStream()
                //.shuffle()
                //.each(new Fields("city", "count"), new S05PrintFilter())
        ;


        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxSpoutPending(1);
        conf.setMessageTimeoutSecs(10000000);


        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, topology.build());
    }
}
