package spout;

import backtype.storm.spout.SpoutOutputCollector;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author liujinxin
 * @since 2015-06-29 10:06
 */
public abstract class KafkaConsumerManager {
    protected KafkaConsumerSpoutConfig config;
    protected KafkaMsgDecoder kafkaMsgDecoder;
    protected AtomicInteger topologyIdleTupleNum;
    protected SpoutOutputCollector spoutOutputCollector;

    /**
     * 构造方法
     *
     * @param config               配置信息
     * @param kafkaMsgDecoder      kafka消息解码器
     * @param topologyIdleTupleNum 拓扑中空闲tuple数量对象
     * @param spoutOutputCollector
     */
    public KafkaConsumerManager(KafkaConsumerSpoutConfig config, KafkaMsgDecoder kafkaMsgDecoder,
                                AtomicInteger topologyIdleTupleNum, SpoutOutputCollector spoutOutputCollector) {
        this.config = config;
        this.kafkaMsgDecoder = kafkaMsgDecoder;
        this.topologyIdleTupleNum = topologyIdleTupleNum;
        this.spoutOutputCollector = spoutOutputCollector;
    }

    /**
     * 开火（启动kafka消费者）
     */
    public abstract void fire();

    /**
     * 让当前线程暂停一会
     * @param millisecond
     */
    public void quietlySleep(long millisecond) {
        try {
            Thread.sleep(millisecond);
        } catch (InterruptedException e) {
        }
    }

    public String getTopic() {
        return config.getTopic();
    }

    public String getConsumerGroup() {
        return config.getConsumerGroup();
    }

    public KafkaMsgDecoder getKafkaMsgDecoder() {
        return kafkaMsgDecoder;
    }

    public AtomicInteger getTopologyIdleTupleNum() {
        return topologyIdleTupleNum;
    }

    public SpoutOutputCollector getSpoutOutputCollector() {
        return spoutOutputCollector;
    }

}
