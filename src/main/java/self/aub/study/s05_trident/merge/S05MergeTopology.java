package self.aub.study.s05_trident.merge;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import self.aub.study.s05_trident.S05PrintFilter;
import storm.trident.Stream;
import storm.trident.TridentTopology;

/**
 * @author liujinxin
 * @since 2015-07-16 00:15
 */
public class S05MergeTopology {
    public static void main(String[] args) {
        TridentTopology topology = new TridentTopology();
        Stream streamA = topology.newStream("streamA", new S05MergeABatchSpout());
        Stream streamB = topology.newStream("streamB", new S05MergeBBatchSpout());
        //topology.merge(streamA,streamB).each(new Fields("batch_id", "city", "index"), new S05PrintFilter());
        topology.merge(new Fields("batch_id","city", "index"), streamA, streamB).each(new Fields("batch_id","city", "index"), new S05PrintFilter());
        Config conf = new Config();
        conf.setMaxSpoutPending(1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, topology.build());
    }
}
