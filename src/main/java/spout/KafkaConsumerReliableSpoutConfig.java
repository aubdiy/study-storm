package spout;

import java.io.Serializable;

/**
 * @author liujinxin
 * @since 2015-06-22 17:07
 */
public class KafkaConsumerReliableSpoutConfig extends KafkaConsumerSpoutConfig implements Serializable {

    /**
     * kafka消费者组在zookeeper中的根节点
     */
    private String zkKafkaConsumerGroupRootPath;

    /**
     * 构造方法
     *
     * @param topic                        要被消费的kafka主题
     * @param consumerThreadNum            消费线程数
     * @param consumerGroup                kafka消费者组名
     * @param zkConnectString              kafka使用的zookeeper链接，例如：zk1:2181,zk2:2181,....
     * @param zkKafkaRootPath              kafka在zookeeper中的根节点
     * @param zkKafkaConsumerGroupRootPath kafka消费者组在zookeeper中的根节点
     */
    public KafkaConsumerReliableSpoutConfig(String topic, String consumerGroup, int consumerThreadNum,
                                            String zkConnectString, String zkKafkaRootPath, String zkKafkaConsumerGroupRootPath) {
        super(topic, consumerGroup, consumerThreadNum, zkConnectString, zkKafkaRootPath);

        if (zkKafkaConsumerGroupRootPath == null
                || zkKafkaConsumerGroupRootPath.isEmpty() || zkKafkaConsumerGroupRootPath.equals("/")) {
            throw new IllegalArgumentException("invalid zk kafka consumer group root znode: null or empty or zk root path");
        } else {
            zkKafkaConsumerGroupRootPath = trim(zkKafkaConsumerGroupRootPath, '/');
            zkKafkaConsumerGroupRootPath = "/".concat(zkKafkaConsumerGroupRootPath);
        }
        this.zkKafkaConsumerGroupRootPath = zkKafkaConsumerGroupRootPath;
    }


    public String getZkKafkaConsumerGroupRootPath() {
        return zkKafkaConsumerGroupRootPath;
    }

}
