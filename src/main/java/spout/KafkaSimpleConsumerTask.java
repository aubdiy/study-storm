package spout;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * @author liujinxin
 * @since 2015-06-23 10:49
 */
@Deprecated
public class KafkaSimpleConsumerTask implements Runnable {

    private String topic;
    private int partitionId;
    private String clientId;
    private String consumerGroup;
    private long connectorVersion;
    private SimpleConsumer consumer;
    private int outputFieldsLength;
    private long nextOffset = 0;
    private StringBuilder emittedCacheKeyBase;
    private int emittedCacheKeyBaseLength;
    private KafkaSimpleConsumerManager manager;
    private FetchRequestBuilder fetchRequestBuilder;


    public KafkaSimpleConsumerTask(KafkaTopicPartitionConnector kafkaTopicPartitionConnector, KafkaSimpleConsumerManager kafkaSimpleConsumerManager) {
        init(kafkaTopicPartitionConnector);
        this.nextOffset = manager.getHaveConsumedOffset(partitionId) + 1;
        this.manager = kafkaSimpleConsumerManager;
        this.outputFieldsLength = kafkaSimpleConsumerManager.getKafkaMsgDecoder().generateFields().size();
    }

    private void init(KafkaTopicPartitionConnector kafkaTopicPartitionConnector) {
        this.topic = kafkaTopicPartitionConnector.getTopic();
        this.consumerGroup = kafkaTopicPartitionConnector.getConsumerGroup();
        this.partitionId = kafkaTopicPartitionConnector.getPartitionId();
        this.clientId = kafkaTopicPartitionConnector.getClientId();
        this.consumer = kafkaTopicPartitionConnector.getSimpleConsumer();
        this.connectorVersion = kafkaTopicPartitionConnector.getVersion();
        this.fetchRequestBuilder = new FetchRequestBuilder();
        fetchRequestBuilder.clientId(clientId);
        this.emittedCacheKeyBase = new StringBuilder().append(partitionId).append('-');
        this.emittedCacheKeyBaseLength = emittedCacheKeyBase.length();
    }


    @Override
    public void run() {
        //TODO 多个WORDER启动spout时，会有并发问题，下个版本解决，此版本只支持一个spout并发
        FetchRequestBuilder fetchRequestBuilder = new FetchRequestBuilder();
        fetchRequestBuilder.clientId(clientId);
        while (true) {
            FetchRequest req = fetchRequestBuilder.addFetch(topic, partitionId, nextOffset, 10000).build();
            // TODO offset不存在了的处理，日志+ 获取新的offset
            FetchResponse fetchResponse = consumer.fetch(req);
            if (fetchResponse.hasError()) {
                retryCreateSimpleConsumer();
                continue;
            } else {
                emitMessage(fetchResponse);
            }
        }
    }

    /**
     * 重新创建消费者连接
     */
    private void retryCreateSimpleConsumer() {
        String clientName = manager.generateSimpleConsumerClientId();
        SimpleConsumer currentConsumer = new SimpleConsumer(this.consumer.host(), this.consumer.port(), 100000, 64 * 1024, clientName);
        List<String> topicList = new ArrayList<>();
        topicList.add(topic);
        boolean connectResult;
        try {
            currentConsumer.send(new TopicMetadataRequest(topicList));
            connectResult = true;
        } catch (Exception e) {
            connectResult = false;
        }
        if (connectResult) {
            //使用当前leader broker 新建连接，适合网络闪断的情况
            fetchRequestBuilder.clientId(clientName);
            this.consumer = currentConsumer;
        } else {
            //重新获取当前topic的对应的partition的leader broker 连接, 适合网络异常及leader broder切换的场景
            KafkaTopicPartitionConnector kafkaTopicPartitionConnector = null;
            while (kafkaTopicPartitionConnector == null) {
                quietlySleep((long) (Math.random() * 50000));
                kafkaTopicPartitionConnector = manager.retryGenerateTopicPartitionConnector(topic, partitionId, connectorVersion);
            }
            init(kafkaTopicPartitionConnector);
        }
    }

    /**
     * 向拓扑中发射tuple
     *
     * @param fetchResponse
     */
    private void emitMessage(FetchResponse fetchResponse) {
        Iterator<MessageAndOffset> iterator = fetchResponse.messageSet(topic, partitionId).iterator();
        while (iterator.hasNext()) {
            if (manager.getTopologyIdleTupleNum().get() < 0) {
                quietlySleep((long) (Math.random() * 10000));
                continue;
            }
            MessageAndOffset messageAndOffset = iterator.next();
            long currentOffset = messageAndOffset.offset();
            long readOffset = messageAndOffset.nextOffset();
            ByteBuffer payload = messageAndOffset.message().payload();
            byte[] bytes = new byte[payload.limit()];
            payload.get(bytes);
            List<Object> tuple = null;
            try {
                tuple = manager.getKafkaMsgDecoder().generateTuple(bytes);
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (tuple == null || tuple.size() != outputFieldsLength) {
                continue;
            }
            KafkaConsumerMsgId messageId = new KafkaConsumerMsgId(consumerGroup, topic, partitionId, currentOffset);
            manager.getSpoutOutputCollector().emit(tuple, messageId);
            manager.getTopologyIdleTupleNum().decrementAndGet();
            nextOffset = readOffset;
            // 发送的消息放到缓存，ack删除，fail重发
            emittedCacheKeyBase.setLength(emittedCacheKeyBaseLength);
            manager.getEmittedCache().put(emittedCacheKeyBase.append(currentOffset).toString(), tuple);
        }
    }


    private void quietlySleep(long millisecond) {
        try {
            Thread.sleep(millisecond);
        } catch (InterruptedException e) {
        }
    }

}
