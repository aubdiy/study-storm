package self.aub.study.s06_metric;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;

public class S06MetricSpout implements IRichSpout {

    private static final long serialVersionUID = 1L;
    private static Logger log = LoggerFactory.getLogger(S06MetricSpout.class);

    private SpoutOutputCollector collector;
    private final int index = 0;
    private int thisTaskIndex;

    private int msgId = 0;

    @Override
    public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.thisTaskIndex = context.getThisTaskIndex();
    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void nextTuple() {
        Utils.sleep(2000);
        final String[] words = new String[]{"nathan", "mike", "jackson", "golda", "bertels"};
        final Random rand = new Random();
        String word = words[rand.nextInt(words.length)];
//        collector.emit(new Values(word));
        collector.emit(new Values(word), ++msgId);
        log.info("spout {} >> {}", thisTaskIndex, word);
    }

    @Override
    public void ack(Object msgId) {
        log.info("sys out ==  ack !!!!!!!!!!! >> {}", msgId);
    }

    @Override
    public void fail(Object msgId) {
        log.info("sys out ==  fail !!!!!!!!!!! >> {}", msgId);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("aa"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
