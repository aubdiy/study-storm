package self.aub.study.s05_trident;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Sum;

import java.util.Map;

/**
 * @author liujinxin
 * @since 2015-07-16 00:15
 */
public class S05HelloTopology {
    public static void main(String[] args) {
        TridentTopology topology = new TridentTopology();
        topology.newStream("test", new S05HelloBatchSpout())

                .each(new Fields("index"), new S05HelloFilter())
                .each(new Fields("batch_id", "city"), new S05HelloFunction(), new Fields("count")).parallelismHint(2)
//                .shuffle()
                .partitionAggregate(new Fields("count"), new Sum(), new Fields("sum"))

                .shuffle()
                .each(new Fields("sum"), new S05PrintFilter())
//                .each(new Fields("batch_id", "city", "index", "count"), new S05PrintFilter())

//              .project(new Fields("index", "city"))
//                .each(new Fields("city", "index"), new S05PrintFilter())


        ;
//


        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxSpoutPending(1);

//        String jarFilePath="/Users/liujinxin/Workspace/idea/study-storm/target/study-storm-1.0.jar";
//        System.setProperty("storm.jar", jarFilePath);
//        conf.put(Config.NIMBUS_HOST, "127.0.0.1");
//        try {
//            StormSubmitter.submitTopology("test", conf, topology.build());
//        } catch (AlreadyAliveException e) {
//            e.printStackTrace();
//        } catch (InvalidTopologyException e) {
//            e.printStackTrace();
//        }

//        LocalCluster cluster = new LocalCluster();

//        cluster.submitTopology("test", conf, topology.build());
        StormTopology build = topology.build();
        for(Map.Entry entry:build.get_spouts().entrySet()){
            System.out.println(entry.getKey());
        }
        System.out.println("=====");
        for(Map.Entry entry:build.get_bolts().entrySet()){
            System.out.println(entry.getKey());
        }
    }
}
