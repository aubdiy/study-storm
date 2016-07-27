package spout;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;

import java.util.Map;

/**
 * @author liujinxin
 * @since 2015-06-29 10:25
 */
public class KafkaConsumerBaseSpout extends KafkaConsumerSpout {
    private KafkaConsumerBaseManager manager;
    private KafkaConsumerSpoutConfig config;

    /**
     * @param config          配置信息
     * @param kafkaMsgDecoder kafka消息解码器
     */
    public KafkaConsumerBaseSpout(KafkaConsumerSpoutConfig config, KafkaMsgDecoder kafkaMsgDecoder) {
        super(kafkaMsgDecoder);
        this.config = config;
        this.kafkaMsgDecoder = kafkaMsgDecoder;
    }


    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        topologyUnackTupleMaxNum = Integer.parseInt(map.get(Config.TOPOLOGY_MAX_SPOUT_PENDING).toString());
        manager = new KafkaConsumerBaseManager(config, kafkaMsgDecoder, topologyIdleTupleNum, spoutOutputCollector);
        manager.fire();
    }
}
