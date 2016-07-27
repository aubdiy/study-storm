package spout;

/**
 * @author liujinxin
 * @since 2015-06-23 13:50
 */
@Deprecated
public class KafkaSimpleConsumerMessageId {
    private String group;
    private String topic;
    private int partition;
    private long offset;

    public KafkaSimpleConsumerMessageId(String group, String topic, int partition, long offset) {
        this.group = group;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
    }

    public String getGroup() {
        return group;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }
}
