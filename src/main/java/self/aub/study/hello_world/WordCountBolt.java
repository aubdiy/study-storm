package self.aub.study.hello_world;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class WordCountBolt implements IRichBolt {
    private static Logger log = LoggerFactory.getLogger(WordCountBolt.class);

    private static final long serialVersionUID = 1L;
    private OutputCollector collector;
    private int thisTaskIndex;

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
        int thisTaskId = context.getThisTaskId();
        int thisTaskIndex = context.getThisTaskIndex();
        this.collector = collector;
        this.thisTaskIndex = context.getThisTaskIndex();
    }

    @Override
    public void execute(Tuple tuple) {
        log.info("log == bolt {} >> {}", thisTaskIndex, tuple.getString(0));
//        System.out.println("sys out == bolt " + thisTaskIndex + " >> " + tuple.getString(0));
//        collector.ack(tuple);
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
