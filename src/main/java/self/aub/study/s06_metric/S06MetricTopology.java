package self.aub.study.s06_metric;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.NotAliveException;
import backtype.storm.topology.TopologyBuilder;
import org.apache.thrift7.TException;

public class S06MetricTopology {
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, NotAliveException, TException {



        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("words", new S06MetricSpout());
        builder.setBolt("hello", new S06MetricBolt()).shuffleGrouping("words");

        Config conf = new Config();
        conf.setNumWorkers(1);
        conf.setNumAckers(1);
        conf.setMessageTimeoutSecs(5);
        conf.setMaxSpoutPending(1);

        conf.registerMetricsConsumer(MyMetricsConsumer.class, 1);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", conf, builder.createTopology());
    }
}
