package spout;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;

import java.util.Map;

/**
 * @author liujinxin
 * @since 2015-06-26 10:51
 */
public class KafkaConsumerReliableSpout extends KafkaConsumerSpout {


    private KafkaConsumerReliableManager manager;
    private KafkaConsumerReliableSpoutConfig config;

    /**
     * @param config          配置信息
     * @param kafkaMsgDecoder kafka消息解码器
     */
    public KafkaConsumerReliableSpout(KafkaConsumerReliableSpoutConfig config, KafkaMsgDecoder kafkaMsgDecoder) {
        super(kafkaMsgDecoder);
        this.config = config;
        this.kafkaMsgDecoder = kafkaMsgDecoder;
    }


    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        topologyUnackTupleMaxNum = Integer.parseInt(map.get(Config.TOPOLOGY_MAX_SPOUT_PENDING).toString());
        boolean haveStarted = false;
        if (map.get("rcp.have.started") == null) {
            haveStarted = false;
            map.put("rcp.have.started", true);
        } else {
            haveStarted = true;
        }
        manager = new KafkaConsumerReliableManager(config, kafkaMsgDecoder, topologyIdleTupleNum, spoutOutputCollector, haveStarted);
        manager.fire();
    }

    @Override
    public void ack(Object o) {
        manager.ack((KafkaConsumerMsgId) o);
    }

    @Override
    public void fail(Object o) {
        manager.fail((KafkaConsumerMsgId) o);
    }
}
