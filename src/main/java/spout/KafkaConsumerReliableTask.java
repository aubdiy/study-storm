package spout;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

import java.util.List;


/**
 * @author liujinxin
 * @since 2015-06-23 10:49
 */
public class KafkaConsumerReliableTask implements Runnable {

    private String topic;
    private String consumerGroup;
    private int outputFieldsLength;
    private KafkaStream<byte[], byte[]> stream;
    private KafkaConsumerReliableManager manager;


    public KafkaConsumerReliableTask(KafkaStream<byte[], byte[]> stream, KafkaConsumerReliableManager manager) {
        this.stream = stream;
        this.manager = manager;
        this.topic = manager.getTopic();
        this.consumerGroup = manager.getConsumerGroup();
        this.outputFieldsLength = manager.getKafkaMsgDecoder().generateFields().size();
    }

    @Override
    public void run() {
        StringBuilder strBuilder = new StringBuilder();
        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
        while (iterator.hasNext()) {
            if (manager.getTopologyIdleTupleNum().get() <= 0) {
               manager.quietlySleep((long) (Math.random() * 10000));
                continue;
            }
            MessageAndMetadata<byte[], byte[]> next = iterator.next();
            byte[] message = next.message();
            List<Object> tuple = null;
            try {
                tuple = manager.getKafkaMsgDecoder().generateTuple(message);
            } catch (Exception e) {
                e.printStackTrace();
            }
            int partition = next.partition();
            long offset = next.offset();
            KafkaConsumerMsgId messageId = new KafkaConsumerMsgId(consumerGroup, topic, partition, offset);
            manager.createUnAckedOffsetZnodeToZk(messageId);
            if (tuple == null || tuple.size() != outputFieldsLength) {
                //TODO 错误消息打印
                continue;
            } else {
                // 发送的消息放到缓存，ack删除，fail重发
                strBuilder.setLength(0);
                manager.getEmittedCache().put(strBuilder.append(partition).append('-').append(offset).toString(), tuple);
                manager.getTopologyIdleTupleNum().decrementAndGet();
                manager.getSpoutOutputCollector().emit(tuple, messageId);
            }
        }
    }
}
