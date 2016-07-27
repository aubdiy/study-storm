package spout;

import backtype.storm.spout.SpoutOutputCollector;
import kafka.cluster.Broker;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 使用kafka低级消费者API实现的管理器
 *
 * @author liujinxin
 * @since 2015-06-19 15:24
 */
@Deprecated
public class KafkaSimpleConsumerManager {

    private KafkaSimpleConsumerSpoutConfig config;
    private KafkaMsgDecoder kafkaMsgDecoder;
    private AtomicInteger topologyIdleTupleNum;
    private SpoutOutputCollector spoutOutputCollector;
    private CuratorFramework curatorFramework;
    private Map<String, List<Object>> emittedCache = new HashMap<>();
    private Map<String, KafkaTopicPartitionConnector> topicPartitionConnectorCache = new HashMap<>();
    private Map<Integer, Queue<Long>> ackedPartitionOffsetQueueMap = new HashMap<>();

    /**
     * 构造方法
     *
     * @param config               配置信息
     * @param kafkaMsgDecoder      kafka消息解码器
     * @param topologyIdleTupleNum 拓扑中空闲tuple数量对象
     * @param spoutOutputCollector
     */
    public KafkaSimpleConsumerManager(KafkaSimpleConsumerSpoutConfig config, KafkaMsgDecoder kafkaMsgDecoder,
                                      AtomicInteger topologyIdleTupleNum, SpoutOutputCollector spoutOutputCollector) {
        this.config = config;
        this.kafkaMsgDecoder = kafkaMsgDecoder;
        this.topologyIdleTupleNum = topologyIdleTupleNum;
        this.spoutOutputCollector = spoutOutputCollector;
        this.curatorFramework = createCuratorFramework();
    }

    /**
     * 消费者启动器，当前消费者组和主题下，每个partition建立一个线程，开始消费数据
     */
    public void fire() {
        List<KafkaTopicPartitionConnector> kafkaTopicPartitionConnectorList = createTopicPartitionConnectorList();
        for (KafkaTopicPartitionConnector kafkaTopicPartitionConnector : kafkaTopicPartitionConnectorList) {
            //TODO kafka重新生成partiion时如何处理？
            ackedPartitionOffsetQueueMap.put(kafkaTopicPartitionConnector.getPartitionId(), new PriorityBlockingQueue<Long>());
        }

        ExecutorService executor = Executors.newFixedThreadPool(kafkaTopicPartitionConnectorList.size());
        for (final KafkaTopicPartitionConnector kafkaTopicPartitionConnector : kafkaTopicPartitionConnectorList) {
            executor.submit(new KafkaSimpleConsumerTask(kafkaTopicPartitionConnector, this));
        }

    }

    /**
     * 重新获取kafka中特定topic下的所有partition的连接
     * @param topic
     * @param partition
     * @param currentVersion
     * @return
     */
    public KafkaTopicPartitionConnector retryGenerateTopicPartitionConnector(String topic, int partition, long currentVersion) {
        //双重检查锁
        String key = new StringBuilder(topic).append('-').append(partition).toString();
        KafkaTopicPartitionConnector kafkaTopicPartitionConnector = topicPartitionConnectorCache.get(key);
        if (currentVersion < kafkaTopicPartitionConnector.getVersion()) {
            return kafkaTopicPartitionConnector;
        }
        synchronized (this) {
            kafkaTopicPartitionConnector = topicPartitionConnectorCache.get(key);
            if (currentVersion < kafkaTopicPartitionConnector.getVersion()) {
                return kafkaTopicPartitionConnector;
            }
            synchronized (this) {
                refreshTopicPartitionConnectorCache();
            }
        }
        kafkaTopicPartitionConnector = topicPartitionConnectorCache.get(key);
        if (currentVersion < kafkaTopicPartitionConnector.getVersion()) {
            return kafkaTopicPartitionConnector;
        } else {
            return null;
        }

    }

    /**
     * 刷新kafka中特定topic下的所有partition的连接缓存
     */
    private void refreshTopicPartitionConnectorCache()  {
        List<KafkaTopicPartitionConnector> kafkaTopicPartitionConnectorList = createTopicPartitionConnectorList();
        StringBuilder keyStrBuilder = new StringBuilder();
        for (KafkaTopicPartitionConnector kafkaTopicPartitionConnector : kafkaTopicPartitionConnectorList) {
            String topic = kafkaTopicPartitionConnector.getTopic();
            int partition = kafkaTopicPartitionConnector.getPartitionId();
            keyStrBuilder.append(topic).append('-').append(partition);
            topicPartitionConnectorCache.put(keyStrBuilder.toString(), kafkaTopicPartitionConnector);
        }
    }

    /**
     * 创建kafka中特定topic下的所有partition的连接，不停的尝试，直到获取到相应的连接
     *
     * @return
     */
    public List<KafkaTopicPartitionConnector> createTopicPartitionConnectorList() {
        while (true) {
            List<KafkaBroker> kafkaBrokerList = getKafkaBrokerList();
            List<KafkaTopicPartitionLeaderBroker> kafkaTopicPartitionLeaderBrokerList = null;
            for (KafkaBroker kafkaBroker : kafkaBrokerList) {
                kafkaTopicPartitionLeaderBrokerList = getTopicPartitionLeaderBrokerList(config.getTopic(), kafkaBroker);
                if (kafkaTopicPartitionLeaderBrokerList != null) {
                    break;
                }
            }
            if (kafkaTopicPartitionLeaderBrokerList == null) {
                continue;
            }
            List<KafkaTopicPartitionConnector> topicPartitionConnectorList = createTopicPartitionConnectorList(kafkaTopicPartitionLeaderBrokerList);
            if (topicPartitionConnectorList != null) {
                return topicPartitionConnectorList;
            }
        }
    }

    /**
     * 根据kafka中特定topic下的所有partition的leader broker信息 创建连接，
     * 该方法为快速失败的，任意一个broker连接不上，直接返回null
     * @param kafkaTopicPartitionLeaderBrokerList
     * @return
     */
    private List<KafkaTopicPartitionConnector> createTopicPartitionConnectorList(List<KafkaTopicPartitionLeaderBroker> kafkaTopicPartitionLeaderBrokerList) {
        List<KafkaTopicPartitionConnector> kafkaTopicPartitionConnectorList = new ArrayList<>(kafkaTopicPartitionLeaderBrokerList.size());
        long version = System.currentTimeMillis();
        for (KafkaTopicPartitionLeaderBroker kafkaTopicPartitionLeaderBroker : kafkaTopicPartitionLeaderBrokerList) {
            String topicPartitionLeaderBrokerHost = kafkaTopicPartitionLeaderBroker.getHost();
            int topicPartitionLeaderBrokerPort = kafkaTopicPartitionLeaderBroker.getPort();
            String clientID = generateSimpleConsumerClientId();
            SimpleConsumer consumer = null;
            try {
                 consumer = new SimpleConsumer(topicPartitionLeaderBrokerHost, topicPartitionLeaderBrokerPort, 100000, 64 * 1024, clientID);
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
            String topic = kafkaTopicPartitionLeaderBroker.getTopic();
            int partitionId = kafkaTopicPartitionLeaderBroker.getPartitionId();
            kafkaTopicPartitionConnectorList.add(new KafkaTopicPartitionConnector(topic, config.getConsumerGroup(),partitionId, clientID, version,consumer));
        }
        return kafkaTopicPartitionConnectorList;
    }

    /**
     * 获取kafka中特定topic下的所有partition的leader broker信息
     *
     * @param topic
     * @param kafkaBroker
     * @return
     */
    private List<KafkaTopicPartitionLeaderBroker> getTopicPartitionLeaderBrokerList(String topic, KafkaBroker kafkaBroker) {
        List<KafkaTopicPartitionLeaderBroker> kafkaTopicPartitionLeaderBrokerList = new ArrayList<>();
        SimpleConsumer consumer = new SimpleConsumer(kafkaBroker.getHost(), kafkaBroker.getPort(), 100000, 64 * 1024, "LeaderBrokerLookup");
        List<String> topics = Collections.singletonList(topic);
        TopicMetadataRequest req = new TopicMetadataRequest(topics);
        TopicMetadataResponse resp;
        try {
            resp = consumer.send(req);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            consumer.close();
        }
        List<TopicMetadata> metaData = resp.topicsMetadata();
        for (TopicMetadata item : metaData) {
            for (PartitionMetadata part : item.partitionsMetadata()) {
                int partitionId = part.partitionId();
                Broker leaderBroker = part.leader();
                String leaderBrokerHost = leaderBroker.host();
                int leaderBrokerPort = leaderBroker.port();
                kafkaTopicPartitionLeaderBrokerList.add(new KafkaTopicPartitionLeaderBroker(topic, partitionId, leaderBrokerHost, leaderBrokerPort));
            }
        }
        return kafkaTopicPartitionLeaderBrokerList;
    }

    /**
     * 生成消费者客户端ID
     * @return
     */
    public String generateSimpleConsumerClientId() {
        String localHostAddress = null;
        try {
            localHostAddress = InetAddress.getLocalHost().toString();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return new StringBuilder(localHostAddress).append('-').append(UUID.randomUUID()).toString();
    }


    /**
     * 获取当前partition已经消费的offset
     * @param partition kafka分区
     * @return
     */
    public long getHaveConsumedOffset(int partition) {
        String topic = config.getTopic();
        String consumerGroup = config.getConsumerGroup();
        String path = new StringBuilder(consumerGroup).append("/offsets").append(topic).append('/').append(partition).toString();
        while (true){
            try {
                curatorFramework.create().creatingParentsIfNeeded().forPath(path, longToByte(0));
                break;
            }catch (KeeperException.NodeExistsException nee){
                nee.printStackTrace();
                break;
            }catch (Exception e) {
                //连接失败 重新连接继续创建
                e.printStackTrace();
            }
        }
        while(true){
            try {
                return byteToLong(curatorFramework.getData().forPath(path));
            } catch (Exception e) {
                //连接失败 重新连接继续创建
                e.printStackTrace();
            }
        }
    }

    /**
     * 拓扑消息成功处理
     *
     * @param messageId
     */
    public void ack(KafkaConsumerMsgId messageId) {
        deleteAckData(messageId);
        updateOffsetToZk(messageId);
    }

    /**
     * 删除缓存tuple
     *
     * @param messageId
     */
    private void deleteAckData(KafkaConsumerMsgId messageId) {
        String key = new StringBuilder().append(messageId.getPartition()).append('-').append(messageId.getOffset()).toString();
        emittedCache.remove(key);
    }

    /**
     * 更新完成的offset到zookeeper
     * 该方法是快速失败的，连接异常不予处理，下次ACK会再次连接
     * @param messageId
     */
    private void updateOffsetToZk(KafkaConsumerMsgId messageId) {
        //多个bolt多个ack线程时，不能保证消息顺序完成， 使用PriorityBlockingQueue逐个更新已经ack的最小的offset
        Queue<Long> ackedPartitionOffsetQueue = ackedPartitionOffsetQueueMap.get(messageId.getPartition());
        ackedPartitionOffsetQueue.add(messageId.getOffset());
        String path = new StringBuilder(messageId.getGroup()).append("/offsets").append(messageId.getTopic()).append('/').append(messageId.getPartition()).toString();
        try {
            Stat historyStat = new Stat();
            long offset = byteToLong(curatorFramework.getData().storingStatIn(historyStat).forPath(path));
            while (++offset >= ackedPartitionOffsetQueue.peek()) {
                curatorFramework.setData().withVersion(historyStat.getVersion()).forPath(path, longToByte(offset));
                if (ackedPartitionOffsetQueue.poll() == null) {
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 拓扑消息失败处理
     *
     * @param messageId
     */
    public void fail(KafkaConsumerMsgId messageId) {
        String key = new StringBuilder().append(messageId.getPartition()).append('-').append(messageId.getOffset()).toString();
        List<Object> tuple = emittedCache.get(key);
        spoutOutputCollector.emit(tuple, messageId);
    }

    /**
     * long转化成byte数组
     *
     * @param number 数值
     * @return
     */
    public byte[] longToByte(long number) {
        long temp = number;
        byte[] byteArr = new byte[8];
        for (int i = 0; i < byteArr.length; i++) {
            byteArr[i] = (byte) (temp & 0xff);// 将最低位保存在最低位
            temp = temp >> 8; // 向右移8位
        }
        return byteArr;
    }

    /**
     * byte数组转成long
     *
     * @param byteArr byte数组
     * @return
     */
    public long byteToLong(byte[] byteArr) {
        long l0 = byteArr[0] & 0xff;// 最低位
        long l1 = byteArr[1] & 0xff;
        long l2 = byteArr[2] & 0xff;
        long l3 = byteArr[3] & 0xff;
        long l4 = byteArr[4] & 0xff;// 最低位
        long l5 = byteArr[5] & 0xff;
        long l6 = byteArr[6] & 0xff;
        long l7 = byteArr[7] & 0xff;
        // s0不变
        l1 <<= 8;
        l2 <<= 16;
        l3 <<= 24;
        l4 <<= 32;
        l5 <<= 40;
        l6 <<= 48;
        l7 <<= 65;
        return l0 | l1 | l2 | l3 | l4 | l5 | l6 | l7;
    }




    /**
     * 创建 CuratorFramework对象
     *
     * @return
     */
    private CuratorFramework createCuratorFramework() {
        String zkConnectString = config.getZkConnectString();
        String zkKafkaConsumerGroupRootZnode = config.getZkKafkaConsumerGroupRootZnode();
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 29);//TODO 数字
        CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(zkConnectString)
                .namespace(zkKafkaConsumerGroupRootZnode)
                .retryPolicy(retryPolicy)
                .build();
        curatorFramework.start();
        return curatorFramework;
    }

    /**
     * 获取kafka集群中的broker
     *
     * @return
     */
    private List<KafkaBroker> getKafkaBrokerList() {
        List<KafkaBroker> result = new ArrayList<>();
        StringBuilder path = new StringBuilder(config.getZkKafkaRootZnode()).append("/brokers/ids");
        List<String> childrenZnodeNameList = null;
        while (true){
            try {
                childrenZnodeNameList = curatorFramework.getChildren().forPath(path.toString());
                break;
            } catch (Exception e) {
                //连接失败 重新连接继续创建
                e.printStackTrace();
            }
        }
        int basePathLength = path.append('/').length();
        for (String childrenZnodeName : childrenZnodeNameList) {
            path.setLength(basePathLength);
            path.append(childrenZnodeName);
            String data = null;
            while (true){

                try {
                    data = new String(curatorFramework.getData().forPath(path.toString()));
                    break;
                } catch (KeeperException.NoNodeException nne){
                    nne.printStackTrace();
                    break;
                }catch (Exception e) {
                    //连接失败 重新连接继续创建
                    e.printStackTrace();
                }
            }
            //{"jmx_port":-1,"timestamp":"1435157042716","host":"localhost","version":1,"port":9993}
            data = data.substring(1, data.length() - 1).replace("\"", "");
            String host = null;
            String port = null;
            for (String ele : data.split(",")) {
                if (ele.startsWith("host:")) {
                    host = ele.substring(5);
                } else if (ele.startsWith("port:")) {
                    port = ele.substring(5);
                }
            }
            if (host != null && port != null) {
                result.add(new KafkaBroker(host, Integer.parseInt(port)));
            }
        }
        return result;
    }






    public KafkaMsgDecoder getKafkaMsgDecoder() {
        return kafkaMsgDecoder;
    }

    public AtomicInteger getTopologyIdleTupleNum() {
        return topologyIdleTupleNum;
    }

    public SpoutOutputCollector getSpoutOutputCollector() {
        return spoutOutputCollector;
    }

    public Map<String, List<Object>> getEmittedCache() {
        return emittedCache;
    }

}
