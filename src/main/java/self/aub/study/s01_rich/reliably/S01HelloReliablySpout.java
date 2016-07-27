package self.aub.study.s01_rich.reliably;

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
public class S01HelloReliablySpout extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(S01HelloReliablySpout.class);
    private static final Map<Integer, String> pending = new HashMap<>();
    private static Integer msgId = 0;
    private SpoutOutputCollector collector;
    private TopologyContext topologyContext;


    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        this.topologyContext = topologyContext;
    }

    @Override
    public void nextTuple() {
        Utils.sleep(5000);
        ++msgId;
        String value = "我是第".concat(msgId.toString());
        collector.emit(new Values(value), msgId);
        pending.put(msgId, value);
        LOG.info("spout emit ========>>  msgId:{},value:{}", msgId, value);
    }


/*
    @Override
    public void nextTuple() {
        int taskIndex = topologyContext.getThisTaskIndex();
        if (taskIndex == 0) {
            Utils.sleep(2000);
            ++msgId;
            String value = "我是第".concat(msgId.toString());
            collector.emit(new Values(value), msgId);
            pending.put(msgId, value);
            LOG.info("spout emit ========>>  msgId:{},value:{}", msgId, value);
        }else{
            Utils.sleep(5000);
            ++msgId;
            String value = "我是第".concat(msgId.toString());
            collector.emit(new Values(value), msgId);
            pending.put(msgId, value);
            LOG.info("spout emit ========>>  msgId:{},value:{}", msgId, value);
        }
    }
*/


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