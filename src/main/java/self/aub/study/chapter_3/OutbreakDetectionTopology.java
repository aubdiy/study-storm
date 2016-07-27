package self.aub.study.chapter_3;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import storm.trident.Stream;
import storm.trident.TridentTopology;

/**
 * @author liujinxin
 * @since 2015-06-07 14:28
 */
public class OutbreakDetectionTopology {
    public static StormTopology buildTopology() {
        TridentTopology topology = new TridentTopology();
        DiagnosisEventSpout spout = new DiagnosisEventSpout();
        Stream inputStream = topology.newStream("event", spout);
        inputStream
                .each(new Fields("event"), new DiseaseFilter())
                .each(new Fields("event"), new CityAssignment(), new Fields("city")).parallelismHint(2)//并行度
                .each(new Fields("event", "city"), new HourAssignment(), new Fields("hour","cityDiseaseHour"))
                .groupBy(new Fields("cityDiseaseHour"))
                .persistentAggregate(new OutbreakTrendFactory(), new Count(), new Fields("count"))
                .newValuesStream()
                .each(new Fields("cityDiseaseHour", "count"), new OutbreakDetector(), new Fields("alert"))
                .each(new Fields("alert"), new DispatchAlert(), new Fields());

        return topology.build();

    }

    public static void main(String[] args) throws InterruptedException {
        Config conf = new Config();
        LocalCluster cluster=new LocalCluster();
        cluster.submitTopology("cbc",conf,buildTopology());
        Thread.sleep(200000);
        cluster.shutdown();
    }

}
