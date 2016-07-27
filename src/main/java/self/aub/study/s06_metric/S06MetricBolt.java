package self.aub.study.s06_metric;

import backtype.storm.metric.api.CountMetric;
import backtype.storm.metric.api.MeanReducer;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.metric.api.ReducedMetric;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class S06MetricBolt extends BaseBasicBolt {
    private static Logger log = LoggerFactory.getLogger(S06MetricBolt.class);

    private static final long serialVersionUID = 1L;
    private int thisTaskIndex;
    private transient CountMetric countMetric;
    private transient ReducedMetric reducedMetric;
    private transient MultiCountMetric multiCountMetric;
    private transient SysErrorMetric sysErrorMetric;


    @Override
    public void prepare(Map stormConf, TopologyContext context) {

        context.getThisTaskId();


        this.thisTaskIndex = context.getThisTaskIndex();

        countMetric = new CountMetric();
        multiCountMetric = new MultiCountMetric();
        reducedMetric = new ReducedMetric(new MeanReducer());
        sysErrorMetric = new SysErrorMetric();
//        context.registerMetric("countMetric", countMetric, 5);
//        context.registerMetric("reducedMetric", reducedMetric, 60);
        context.registerMetric("sysErrorMetric", sysErrorMetric, 15);
//        context.registerMetric("multiCountMetric", multiCountMetric, 10);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
//        countMetric.incr();
//        multiCountMetric.scope(tuple.getString(0)).incr();
//        reducedMetric.update(tuple.getString(0).length());
        sysErrorMetric.collect("test error1");
        sysErrorMetric.collect("test error2");
        log.info("bolt {} >> {}", thisTaskIndex, tuple.getString(0));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }



    @Override
    public void cleanup() {

    }

}
