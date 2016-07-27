package self.aub.study.s05_trident.repartitioning;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import org.apache.storm.http.entity.FileEntity;
import storm.trident.TridentTopology;

/**
 * @author liujinxin
 * @since 2015-07-16 00:15
 */
public class S05RepartitioningTopology {
    public static void main(String[] args) {
        TridentTopology topology = new TridentTopology();
        topology.newStream("test", new S05RepartitioningBatchSpout()).parallelismHint(1)
                .shuffle()
                .each(new Fields("batch_id", "city", "index"), new S05RepartitioningShuffleFilter()).parallelismHint(3)

                .broadcast()
                .each(new Fields("city", "index"), new S05RepartitioningBroadcastFilter()).parallelismHint(2)

                .partitionBy(new Fields("index"))
                .each(new Fields("batch_id", "city", "index"), new S05RepartitioningPartitionByFilter()).parallelismHint(3)

                .global()
                .each(new Fields("batch_id", "city", "index"), new S05RepartitioningGlobalFilter()).parallelismHint(2)

                .batchGlobal()
                .each(new Fields("batch_id", "city", "index"), new S05RepartitioningBatchGlobalFilter()).parallelismHint(2)

                .partition(new S05CustomRepartitioning())
                .each(new Fields("batch_id", "city", "index"), new S05RepartitioningCustomFilter()).parallelismHint(2)

                .groupBy(new Fields("index"))
                .each(new Fields("batch_id", "city", "index"), new S05RepartitioningGroupByFunction(),new Fields("empty"))
        ;

        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxSpoutPending(1);


        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, topology.build());
    }
}
