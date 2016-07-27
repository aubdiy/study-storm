package self.aub.study.s05_trident.aggregator;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import self.aub.study.s05_trident.S05PrintFilter;
import storm.trident.TridentTopology;

/**
 * @author liujinxin
 * @since 2015-07-16 00:15
 */
public class S05AggregatorTopology {

    public static TridentTopology common() {
        TridentTopology topology = new TridentTopology();
        topology.newStream("test", new S05AggregatorBatchSpout())

                .partitionBy(new Fields("index")).parallelismHint(2)
                .aggregate(new Fields("batch_id", "city", "index"), new S05CountCombinerAggregator(), new Fields("count"))
                .aggregate(new Fields("batch_id", "city", "index"), new S05CountReducerAggregator(), new Fields("count"))
                .aggregate(new Fields("batch_id", "city", "index"), new S05CountAggregator(), new Fields("count"))

//                .partitionBy(new Fields("index"))
//                .partitionAggregate(new Fields("city"), new S05CountReducerAggregator(), new Fields("count")).parallelismHint(4)
//                .partitionAggregate(new Fields("city"), new S05CountCombinerAggregator(), new Fields("count")).parallelismHint(4)


                .shuffle()
                .each(new Fields("count"), new S05PrintFilter())
        ;
        return topology;
    }

    public static TridentTopology chain() {
        TridentTopology topology = new TridentTopology();
        topology.newStream("test", new S05AggregatorBatchSpout())

                .chainedAgg()
                .aggregate(new Fields("batch_id", "city", "index"), new S05CountCombinerAggregator(), new Fields("combiner-count"))
                .aggregate(new Fields("batch_id", "city", "index"), new S05CountReducerAggregator(), new Fields("reducer-count"))
                .aggregate(new Fields("batch_id", "city", "index"), new S05CountAggregator(), new Fields("agg-count"))
                .chainEnd()
                .parallelismHint(2)

                .shuffle()
                .each(new Fields("combiner-count", "reducer-count", "agg-count"), new S05PrintFilter())
        ;
        return topology;
    }


    public static void main(String[] args) {
        TridentTopology topology = common();
//        TridentTopology topology = chain();

        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxSpoutPending(1);


        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, topology.build());
    }
}
