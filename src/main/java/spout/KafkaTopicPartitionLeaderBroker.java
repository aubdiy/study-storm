package spout;

/**
 * @author liujinxin
 * @since 2015-06-19 18:46
 */
@Deprecated
public class KafkaTopicPartitionLeaderBroker extends KafkaBroker{
    private String topic;
    private int partitionId;

    public KafkaTopicPartitionLeaderBroker(String topic, int partitionId, String host, int port) {
        super(host, port);
        this.topic=topic;
        this.partitionId = partitionId;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartitionId() {
        return partitionId;
    }
}
