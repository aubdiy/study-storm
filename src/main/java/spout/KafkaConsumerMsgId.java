package spout;

/**
 * @author liujinxin
 * @since 2015-06-23 13:50
 */
public class KafkaConsumerMsgId implements Comparable<KafkaConsumerMsgId> {
    private String group;
    private String topic;
    private int partition;
    private long offset;

    public KafkaConsumerMsgId(String group, String topic, int partition, long offset) {
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


    @Override
    public int compareTo(KafkaConsumerMsgId other) {
        if (this.offset == other.getOffset()) {
            return 0;
        }
        return this.offset < other.getOffset() ? 0 : 1;
    }

    @Override
    public String toString() {
        return group+","+topic+","+partition+","+offset; //TODO
    }
}
