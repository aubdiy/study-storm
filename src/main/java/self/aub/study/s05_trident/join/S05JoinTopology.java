package self.aub.study.s05_trident.join;

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
public class S05JoinTopology {
    public static void main(String[] args) {
        TridentTopology topology = new TridentTopology();
        Stream streamA = topology.newStream("streamA", new S05JoinABatchSpout());
        Stream streamB = topology.newStream("streamB", new S05JoinBBatchSpout());
        //topology.merge(streamA,streamB).each(new Fields("batch_id", "city", "index"), new S05PrintFilter());

        topology.join(
                streamA, new Fields("batch_id_A"),
                streamB, new Fields("batch_id_B"),
                new Fields("batch_id_A", "city_a", "index_a", "city_b", "index_b"))
                .each(new Fields("batch_id_A", "city_a", "city_b"), new S05PrintFilter())
        ;

        //topology.join(
        //        streamA, new Fields("batch_id_A"),
        //        streamB, new Fields("batch_id_B"),
        //        new Fields("batch_id_A", "city_a", "index_a", "city_b", "index_b"),
        //        JoinType.OUTER)
        //        .each(new Fields("batch_id_A", "city_a", "city_b"), new S05PrintFilter())
        //;




        Config conf = new Config();
        conf.setMaxSpoutPending(1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, topology.build());
    }
}
