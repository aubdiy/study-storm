package spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author liujinxin
 * @since 2015-06-29 01:15
 */
public abstract class KafkaConsumerSpout extends BaseRichSpout {
    /**
     * storm拓扑中，未处理完的tuple最大数量
     */
    protected int topologyUnackTupleMaxNum;
    /**
     * storm拓扑中，空闲的tuple数量
     */
    protected final AtomicInteger topologyIdleTupleNum;
    protected KafkaMsgDecoder kafkaMsgDecoder;

    /**
     * @param kafkaMsgDecoder kafka消息解码器
     */
    public KafkaConsumerSpout(KafkaMsgDecoder kafkaMsgDecoder) {
        this.kafkaMsgDecoder = kafkaMsgDecoder;
        this.topologyIdleTupleNum = new AtomicInteger();
    }


    @Override
    public abstract void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector);

    @Override
    public void nextTuple() {
        if (topologyIdleTupleNum.get() < topologyUnackTupleMaxNum) {
            topologyIdleTupleNum.incrementAndGet();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(kafkaMsgDecoder.generateFields());
    }


}
