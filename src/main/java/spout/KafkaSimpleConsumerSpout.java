package spout;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author liujinxin
 * @since 2015-06-22 17:07
 */
@Deprecated
public class KafkaSimpleConsumerSpout implements IRichSpout {
    /**
     * storm拓扑中，未处理完的tuple最大数量
     */
    private int topologyUnackTupleMaxNum; //TODO 二期支持多worker时，topologyUnackTupleMaxNum /= worker数
    /**
     * storm拓扑中，空闲的tuple数量
     */
    private final AtomicInteger topologyIdleTupleNum;
    private KafkaMsgDecoder kafkaMsgDecoder;
    private KafkaSimpleConsumerManager manager;
    private KafkaSimpleConsumerSpoutConfig config;

    /**
     *
     * @param config 配置信息
     * @param kafkaMsgDecoder kafka消息解码器
     */
    public KafkaSimpleConsumerSpout(KafkaSimpleConsumerSpoutConfig config, KafkaMsgDecoder kafkaMsgDecoder) {
        this.config = config;
        this.kafkaMsgDecoder = kafkaMsgDecoder;
        this.topologyIdleTupleNum = new AtomicInteger();
    }


    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        topologyUnackTupleMaxNum = Integer.parseInt(map.get(Config.TOPOLOGY_MAX_SPOUT_PENDING).toString());
        manager = new KafkaSimpleConsumerManager(config, kafkaMsgDecoder, topologyIdleTupleNum, spoutOutputCollector);
        manager.fire();
    }

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


    @Override
    public void ack(Object o) {
        manager.ack((KafkaConsumerMsgId) o);
    }

    @Override
    public void fail(Object o) {
        manager.fail((KafkaConsumerMsgId) o);
    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void close() {
        //TODO 使用manager方法 关闭链接
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
