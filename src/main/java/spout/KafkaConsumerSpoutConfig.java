package spout;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author liujinxin
 * @since 2015-06-29 09:36
 */
public class KafkaConsumerSpoutConfig implements Serializable {
    /**
     * 要被消费的kafka主题
     */
    private String topic;
    /**
     * kafka消费者组名
     */
    private String consumerGroup;
    /**
     * kafka消费者线程数
     */
    private int consumerThreadNum;
    /**
     * kafka使用的zookeeper链接
     */
    private String zkConnectString;
    /**
     * kafka在zookeeper中的根节点
     */
    private String zkKafkaRootPath;


    /**
     * 构造方法
     *
     * @param topic             要被消费的kafka主题
     * @param consumerThreadNum 消费线程数
     * @param consumerGroup     kafka消费者组名
     * @param zkConnectString   kafka使用的zookeeper链接，例如：zk1:2181,zk2:2181,....
     * @param zkKafkaRootPath   kafka在zookeeper中的根节点
     */
    public KafkaConsumerSpoutConfig(String topic, String consumerGroup, int consumerThreadNum, String zkConnectString, String zkKafkaRootPath) {
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("invalid topic: null or empty");
        }
        this.topic = topic;
        if (consumerGroup == null || consumerGroup.isEmpty()) {
            throw new IllegalArgumentException("invalid consumer group: null or empty");
        }
        this.consumerGroup = consumerGroup;
        if (consumerThreadNum < 0) {
            throw new IllegalArgumentException("invalid consumer thread num : must > 0");
        }
        this.consumerThreadNum = consumerThreadNum;
        if (zkConnectString == null || zkConnectString.isEmpty()) {
            throw new IllegalArgumentException("invalid zk connect: null or empty");
        }
        this.zkConnectString = zkConnectString;
        if (zkKafkaRootPath == null || zkKafkaRootPath.equals("/")) {
            zkKafkaRootPath = "";
        } else {
            zkKafkaRootPath = trim(zkKafkaRootPath, '/');
            zkKafkaRootPath = "/".concat(zkKafkaRootPath);
        }
        this.zkKafkaRootPath = zkKafkaRootPath;

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

    public String getZkKafkaRootPath() {
        return zkKafkaRootPath;
    }


    /**
     * 获取kafka消费者配置
     *
     * @return
     */
    public Properties getProperties() {
        Properties properties = new Properties();
        properties.put("zookeeper.connect", zkConnectString.concat(zkKafkaRootPath));
        properties.put("group.id", consumerGroup);
        return properties;
    }

    /**
     * 获取kafka中topic与消费者线程数对应关系map
     *
     * @return
     */
    public Map<String, Integer> getTopicConsumerThreaNumdMap() {
        Map<String, Integer> map = new HashMap<>();
        map.put(topic, consumerThreadNum);
        return map;
    }

    /**
     * 修剪指定字符
     *
     * @param value 要被修剪的字符串
     * @param c     要修剪掉得字符
     * @return
     */
    protected String trim(String value, char c) {
        if (value == null) {
            return null;
        }
        int length = value.length();
        int startIndex = 0;
        while ((startIndex < length) && (value.charAt(startIndex) == c)) {
            startIndex++;
        }
        while ((startIndex < length) && (value.charAt(length - 1) == c)) {
            length--;
        }
        return ((startIndex > 0) || (length < value.length())) ? value.substring(startIndex, length) : value;
    }
}