package self.aub.study.s05_trident.state.transactional;

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
public class S05TransactionalTotology {


    public static void main(String[] args) {
        TridentTopology topology = new TridentTopology();
        //TridentState tridentState = topology.newStaticState(new S05BasicStateFactory());
        topology.newStream("test", new S05TransactionalSpout()).parallelismHint(2)

                //.stateQuery(tridentState, new Fields("city"), new S05BasicQuery(), new Fields("population"))
                //.each(new Fields("tx", "city", "population", "index"), new S05PrintFilter())

                //.partitionPersist(new S05BasicStateFactory(), new Fields("city"), new S05BasicUpdate(), new Fields("city", "population"))
                //.newValuesStream()

                //.shuffle()
                //.each(new Fields("city", "population"), new S05PrintFilter())


                .groupBy(new Fields("tx", "city"))
                .persistentAggregate(new S05TransactionalStateFactory(), new Count(), new Fields("count")).parallelismHint(3)
                .newValuesStream()
                .shuffle()
                .each(new Fields("tx","city", "count"), new S05PrintFilter())
        ;


        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxSpoutPending(1);
        conf.setMessageTimeoutSecs(10);


        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, topology.build());
    }
}
