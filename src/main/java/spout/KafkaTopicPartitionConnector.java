package spout;

import kafka.javaapi.consumer.SimpleConsumer;


/**
 * @author liujinxin
 * @since 2015-06-22 10:33
 */
@Deprecated
public class KafkaTopicPartitionConnector {
    private String topic;
    private String consumerGroup;
    private int partitionId;
    private String clientId;
    private long version;
    private SimpleConsumer simpleConsumer;

    /**
     *
     * @param topic
     * @param consumerGroup
     * @param partitionId
     * @param clientId
     * @param version
     * @param simpleConsumer
     */
    public KafkaTopicPartitionConnector(String topic, String consumerGroup, int partitionId, String clientId, long version, SimpleConsumer simpleConsumer) {
        this.topic = topic;
        this.consumerGroup = consumerGroup;
        this.partitionId = partitionId;
        this.clientId = clientId;
        this.version = version;
        this.simpleConsumer = simpleConsumer;
    }

    public String getTopic() {
        return topic;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public String getClientId() {
        return clientId;
    }

    public long getVersion() {
        return version;
    }

    public SimpleConsumer getSimpleConsumer() {
        return simpleConsumer;
    }
}
