package self.aub.study.hello_world;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.NotAliveException;
import backtype.storm.topology.TopologyBuilder;
import org.apache.thrift7.TException;

public class WordCountLaunch {
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, NotAliveException, TException {


        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("words", new WordCountSpout());
        builder.setBolt("hello", new WordCountBolt()).shuffleGrouping("words");


        // builder.setBolt("exclaim2", new WordBolt(),
        // 2).shuffleGrouping("exclaim1");

        Config conf = new Config();
//        conf.setDebug(true);
//        conf.setNumWorkers(2);
        conf.setNumWorkers(1);
        conf.setNumAckers(1);
        conf.setMessageTimeoutSecs(3);
        conf.setMaxSpoutPending(10);

//        String jarFilePath="/Users/liujinxin/Workspace/idea/study-storm/target/study-storm-1.0.jar";
//        String jarFilePath="/home/sre/liujinxin/study-bigdata-storm.jar";
//        System.setProperty("storm.jar", jarFilePath);
//        conf.put(Config.NIMBUS_HOST, "127.0.0.1");
//        conf.put(Config.NIMBUS_HOST, "10.77.135.52");
//        conf.put(Config.NIMBUS_THRIFT_PORT, 10627);

//        Map conf1 = Utils.readStormConfig();
//        conf1.putAll(conf);
//        NimbusClient client = NimbusClient.getConfiguredClient(conf1);
//        client.getClient().killTopology("test-word-count");
//        client.getClient().activate();
//        client.getClient().deactivate();
//        client.getClient().rebalance();

//       Object obj= client.;
//        System.out.println(obj);

//        conf.registerMetricsConsumer(LoggingMetricsConsumer.class);

//        StormSubmitter.submitTopology("test-word-count", conf,
//                builder.createTopology());
//        try{
//
//            StormSubmitter.submitTopology("-12test-1", conf,builder.createTopology());
//        }catch (Exception e){
//            e.printStackTrace();
//            System.out.println(1111111111);
//        }
//
        String arg = args[0];
        StormSubmitter.submitTopology(arg, conf, builder.createTopology());
        //StormSubmitter.submitTopology("test-word-count", conf, builder.createTopology());

        //LocalCluster cluster = new LocalCluster();
        //cluster.submitTopology("test", conf, builder.createTopology());
    }
}
