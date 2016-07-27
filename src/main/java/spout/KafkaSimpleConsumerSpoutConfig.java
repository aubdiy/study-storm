package spout;

/**
 * @author liujinxin
 * @since 2015-06-22 17:07
 */
@Deprecated
public class KafkaSimpleConsumerSpoutConfig {
    /**
     * 要被消费的kafka主题
     */
    private String topic;
    /**
     * kafka消费者组名
     */
    private String consumerGroup;
    /**
     * kafka使用的zookeeper链接
     */
    private String zkConnectString;
    /**
     * kafka在zookeeper中的根节点
     */
    private String zkKafkaRootZnode;
    /**
     * kafka消费者组在zookeeper中的根节点
     */
    private String zkKafkaConsumerGroupRootZnode;

    /**
     * 构造方法
     * @param topic 要被消费的kafka主题
     * @param consumerGroup kafka消费者组名
     * @param zkConnectString kafka使用的zookeeper链接，例如：zk1:2181,zk2:2181,....
     * @param zkKafkaRootZnode kafka在zookeeper中的根节点
     * @param zkKafkaConsumerGroupRootZnode kafka消费者组在zookeeper中的根节点
     */
    public KafkaSimpleConsumerSpoutConfig(String topic, String consumerGroup, String zkConnectString, String zkKafkaRootZnode, String zkKafkaConsumerGroupRootZnode) {
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("invalid topic: null or empty");
        }
        this.topic = topic;
        if (consumerGroup == null || consumerGroup.isEmpty()) {
            throw new IllegalArgumentException("invalid consumer group: null or empty");
        }
        this.consumerGroup = consumerGroup;
        if (zkConnectString == null || zkConnectString.isEmpty()) {
            throw new IllegalArgumentException("invalid zk connect: null or empty");
        }
        this.zkConnectString = zkConnectString;
        if (zkKafkaRootZnode == null || zkKafkaRootZnode.isEmpty()) {
            throw new IllegalArgumentException("invalid zk kafka root znode: null or empty");
        }
        this.zkKafkaRootZnode = zkKafkaRootZnode;
        if (zkKafkaConsumerGroupRootZnode == null || zkKafkaConsumerGroupRootZnode.isEmpty()) {
            throw new IllegalArgumentException("invalid zk kafka consumer group root znode: null or empty");
        }else if (zkKafkaConsumerGroupRootZnode.length() == 1 && zkKafkaConsumerGroupRootZnode.charAt(0) == '/') {
            throw new IllegalArgumentException("invalid zk kafka consumer group root znode: must not zk root");
        }else {
            if (zkKafkaConsumerGroupRootZnode.charAt(0) != '/') {
                zkKafkaConsumerGroupRootZnode = "/".concat(zkKafkaConsumerGroupRootZnode);
            }
        }
        this.zkKafkaConsumerGroupRootZnode = zkKafkaConsumerGroupRootZnode;
    }

    public String getTopic() {
        return topic;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public String getZkConnectString() {
        return zkConnectString;
    }

    public String getZkKafkaRootZnode() {
        return zkKafkaRootZnode;
    }

    public String getZkKafkaConsumerGroupRootZnode() {
        return zkKafkaConsumerGroupRootZnode;
    }
}
