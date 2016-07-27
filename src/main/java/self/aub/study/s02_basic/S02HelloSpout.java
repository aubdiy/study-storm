package self.aub.study.s02_basic;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author liujinxin
 * @since 2015-07-13 10:21
 */
public class S02HelloSpout extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(S02HelloSpout.class);
    private static final Map<Integer, String> pending = new HashMap<>();
    private static Integer msgId = 0;
    private SpoutOutputCollector collector;


    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;

    }

    @Override
    public void nextTuple() {
        Utils.sleep(5000);
        ++msgId;
        String value = "我是第".concat(msgId.toString());
        pending.put(msgId, value);
        collector.emit(new Values(value), msgId);
        LOG.info("spout emit ========>>  msgId:{},value:{}", msgId, value);
    }

    @Override
    public void ack(Object msgId) {
        LOG.info("spout ack ========>>  msgId:{} ", msgId);
        pending.remove(msgId);
    }

    @Override
    public void fail(Object msgId) {
        LOG.info("spout fail ========>>  msgId:{} ", msgId);
        collector.emit(new Values(pending.get(msgId)), msgId);
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("msg"));
    }


}