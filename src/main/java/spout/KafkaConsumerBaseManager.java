package spout;

import backtype.storm.spout.SpoutOutputCollector;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author liujinxin
 * @since 2015-06-29 10:00
 */
public class KafkaConsumerBaseManager extends KafkaConsumerManager {


    /**
     * 构造方法
     *
     * @param config               配置信息
     * @param kafkaMsgDecoder      kafka消息解码器
     * @param topologyIdleTupleNum 拓扑中空闲tuple数量对象
     * @param spoutOutputCollector
     */
    public KafkaConsumerBaseManager(KafkaConsumerSpoutConfig config, KafkaMsgDecoder kafkaMsgDecoder,
                                    AtomicInteger topologyIdleTupleNum, SpoutOutputCollector spoutOutputCollector) {
        super(config, kafkaMsgDecoder, topologyIdleTupleNum, spoutOutputCollector);
    }

    /**
     * 启动消费线程
     */
    public void fire() {
        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(new ConsumerConfig(config.getProperties()));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector
                .createMessageStreams(config.getTopicConsumerThreaNumdMap());
        for (Map.Entry<String, List<KafkaStream<byte[], byte[]>>> entry : consumerMap.entrySet()) {
            List<KafkaStream<byte[], byte[]>> streamList = entry.getValue();
            ExecutorService executor = Executors.newFixedThreadPool(streamList.size());
            for (final KafkaStream<byte[], byte[]> stream : streamList) {
                executor.submit(new KafkaConsumerBaseTask(stream, this));
            }
        }
    }
}
