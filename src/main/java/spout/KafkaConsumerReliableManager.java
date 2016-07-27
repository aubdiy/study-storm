package spout;

import backtype.storm.spout.SpoutOutputCollector;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.common.ErrorMapping;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author liujinxin
 * @since 2015-06-26 10:53
 */
public class KafkaConsumerReliableManager extends KafkaConsumerManager {
    /**
     * 是否已经启动过（storm集群在运行过程中，处于某种原因，重启spout）
     */
    private boolean haveStarted;
    /**
     * zookeeper操作类
     */
    private CuratorFramework curatorFramework;
    /**
     * zookeeper中记录消费者组信息的路径
     */
    private String zkKafkaConsumerGroupRootPath;
    /**
     * 已发射的tuple缓存
     */
    private Map<String, List<Object>> emittedCache = new HashMap<>();

    /**
     * 构造方法
     *
     * @param config               配置信息
     * @param kafkaMsgDecoder      kafka消息解码器
     * @param topologyIdleTupleNum 拓扑中空闲tuple数量对象
     * @param spoutOutputCollector
     */
    public KafkaConsumerReliableManager(KafkaConsumerReliableSpoutConfig config, KafkaMsgDecoder kafkaMsgDecoder,
                                        AtomicInteger topologyIdleTupleNum, SpoutOutputCollector spoutOutputCollector, boolean haveStarted) {
        super(config, kafkaMsgDecoder, topologyIdleTupleNum, spoutOutputCollector);
        this.haveStarted = haveStarted;
        this.curatorFramework = createCuratorFramework();
        this.zkKafkaConsumerGroupRootPath = config.getZkKafkaConsumerGroupRootPath();
    }

    /**
     * 启动消费线程
     */
    public void fire() {
        //补录数据
        emitUnAckedoffset();
        //正常消费
        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(new ConsumerConfig(config.getProperties()));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector
                .createMessageStreams(config.getTopicConsumerThreaNumdMap());
        for (Map.Entry<String, List<KafkaStream<byte[], byte[]>>> entry : consumerMap.entrySet()) {
            List<KafkaStream<byte[], byte[]>> streamList = entry.getValue();
            ExecutorService executor = Executors.newFixedThreadPool(streamList.size());
            for (final KafkaStream<byte[], byte[]> stream : streamList) {
                executor.submit(new KafkaConsumerReliableTask(stream, this));
            }
        }
    }

    /**
     * 创建 CuratorFramework对象
     *
     * @return
     */
    private CuratorFramework createCuratorFramework() {
        String zkConnectString = config.getZkConnectString();
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(10000, 5);
        CuratorFramework curatorFramework = CuratorFrameworkFactory.builder().connectString(zkConnectString).retryPolicy(retryPolicy).build();
        curatorFramework.start();
        return curatorFramework;
    }

    /**
     * 获取当前topic的partition数
     *
     * @return
     */
    private List<Integer> getKafkaTopicPartitionList() {
        String zkKfakaRootPaht = config.getZkKafkaRootPath();
        StringBuilder path = new StringBuilder(zkKfakaRootPaht).append("/brokers/topics/").append(config.getTopic()).append("/partitions");
        List<String> childrenZnodeNameList = null;
        while (true) {
            try {
                childrenZnodeNameList = curatorFramework.getChildren().forPath(path.toString());
                break;
            } catch (Exception e) {
                //连接失败 重新连接继续创建
                e.printStackTrace();
            }
        }
        List<Integer> result = new ArrayList<>();
        for (String childrenZnodeName : childrenZnodeNameList) {
            result.add(Integer.parseInt(childrenZnodeName));
        }
        return result;
    }

    /**
     * 重新发送上次未发完的数据
     */
    private void emitUnAckedoffset() {
        if (!haveStarted) {
            //正常启动拓扑时执行（storm集群在运行过程中，处于某种原因，重启spout, 需要跳过重发数据阶段）
            List<Integer> partitionList = getKafkaTopicPartitionList();
            for (Integer partition : partitionList) {
                emitUnAckedoffset(partition);
            }
        }
    }

    /**
     * 重新发送上次未发完的数据
     *
     * @param partition topic分区
     */
    private void emitUnAckedoffset(int partition) {
        List<String> unAckedOffsetList = getUnAckedOffsetList(partition);
        if (unAckedOffsetList == null || unAckedOffsetList.size() == 0) {
            return;
        }
        SimpleConsumer consumer = null;
        FetchRequestBuilder fetchRequestBuilder = new FetchRequestBuilder();
        long haveConsumedOffset = getHaveConsumedOffset(partition);
        for (int i = 0; i < unAckedOffsetList.size(); ) {
            long offset = Long.parseLong(unAckedOffsetList.get(i));
            if (offset > haveConsumedOffset) {
                ++i;
                continue;
            }
            if (consumer == null) {
                String topicPartitionLeaderBrokerId = getTopicPartitionLeaderBrokerId(partition);
                KafkaBroker kafkaBroker = getKafkaBroker(topicPartitionLeaderBrokerId);
                String clientId = generateSimpleConsumerClientId();
                try {
                    consumer = new SimpleConsumer(kafkaBroker.getHost(), kafkaBroker.getPort(), 100000, 64 * 1024, clientId);
                } catch (Exception e) {
                    continue;
                }
                fetchRequestBuilder.clientId(clientId);
            }
            FetchRequest req = fetchRequestBuilder.addFetch(config.getTopic(), partition, offset, 10000).build();
            FetchResponse fetchResponse = consumer.fetch(req);
            if (fetchResponse.hasError()) {
                if (fetchResponse.errorCode(config.getTopic(), partition) == ErrorMapping.OffsetOutOfRangeCode()) {
//                   TODO 超出范围时 打 日志
                    ++i;
                } else {
                    consumer.close();
                    consumer = null;
                }
            } else {
                emitMessage(fetchResponse, partition);
                ++i;
            }
        }
        consumer.close();
    }

    /**
     * 获取没有被acked的offset
     *
     * @param partition topic分区
     * @return
     */
    private List<String> getUnAckedOffsetList(int partition) {
        String path = new StringBuilder(zkKafkaConsumerGroupRootPath).append('/').append(config.getConsumerGroup())
                .append("/offsets/").append(config.getTopic()).append('/').append(partition).toString();
        List<String> unAckedOffsetList = null;
        Stat stat = new Stat();
        do {
            try {
                unAckedOffsetList = curatorFramework.getChildren().storingStatIn(stat).forPath(path);
                curatorFramework.setData().withVersion(stat.getVersion()).forPath(path);
            } catch (KeeperException.BadVersionException bve) {
                //更新失败，说明有别的进程/线程 已经开始处理，此进程不许处理此partition
                return null;
            } catch (KeeperException.NoNodeException nne) {
                //节点不存在，说明还没未被ack的数据，
                return null;
            } catch (Exception e) {
                e.printStackTrace();
            }
        } while (unAckedOffsetList == null);
        return unAckedOffsetList;
    }

    /**
     * 获取当前topic分区的leader borkerID
     *
     * @param partition topic分区
     * @return
     */
    private String getTopicPartitionLeaderBrokerId(int partition) {
        StringBuilder path = new StringBuilder(config.getZkKafkaRootPath()).append("/brokers/topics/")
                .append(config.getTopic()).append("/partitions/").append(partition).append("state");
        String data = null;
        while (true) {
            try {
                data = new String(curatorFramework.getData().forPath(path.toString()));
                break;
            } catch (KeeperException.NoNodeException nne) {
                nne.printStackTrace();
                break;
            } catch (Exception e) {
                //连接失败 重新连接继续创建
                e.printStackTrace();
            }
        }
        //{"controller_epoch":38,"leader":2,"version":1,"leader_epoch":28,"isr":[2]}
        data = data.substring(1, data.length() - 1).replace("\"", "");
        String leaderBrokerId = null;
        for (String ele : data.split(",")) {
            if (ele.startsWith("leader:")) {
                leaderBrokerId = ele.substring(7);
                break;
            }
        }
        return leaderBrokerId;
    }

    /**
     * 根据broderid 获取kafka的broke信息
     *
     * @param brokerId kafaka的brokerID
     * @return
     */
    private KafkaBroker getKafkaBroker(String brokerId) {
        StringBuilder path = new StringBuilder(config.getZkKafkaRootPath()).append("/brokers/ids/").append(brokerId);
        String data = null;
        while (true) {
            try {
                data = new String(curatorFramework.getData().forPath(path.toString()));
                break;
            } catch (KeeperException.NoNodeException nne) {
                nne.printStackTrace();
                break;
            } catch (Exception e) {
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

            KafkaBroker kafkaBroker = null;
            if (host != null && port != null) {
                kafkaBroker = new KafkaBroker(host, Integer.parseInt(port));
            }
            return kafkaBroker;
        }
        return null;
    }

    /**
     * 获取指定分区的已经被消费过的offset
     *
     * @param partition topic分区
     * @return
     */
    private long getHaveConsumedOffset(int partition) {
        StringBuilder path = new StringBuilder(config.getZkKafkaRootPath()).append("/consumers/").append(config.getConsumerGroup())
                .append("/offsets/").append(config.getTopic()).append('/').append(partition);
        String data = null;
        while (true) {
            try {
                data = new String(curatorFramework.getData().forPath(path.toString()));
                break;
            } catch (KeeperException.NoNodeException nne) {
                nne.printStackTrace();
                break;
            } catch (Exception e) {
                //连接失败 重新连接继续创建
                e.printStackTrace();
            }
        }
        return Long.parseLong(data);
    }

    /**
     * 向拓扑中发射tuple
     *
     * @param fetchResponse
     */
    private void emitMessage(FetchResponse fetchResponse, int partition) {
        Iterator<MessageAndOffset> iterator = fetchResponse.messageSet(config.getTopic(), partition).iterator();
        while (iterator.hasNext()) {
            if (topologyIdleTupleNum.get() < 0) {
                quietlySleep((long) (Math.random() * 10000));
                continue;
            }
            MessageAndOffset messageAndOffset = iterator.next();
            long currentOffset = messageAndOffset.offset();
            ByteBuffer payload = messageAndOffset.message().payload();
            byte[] bytes = new byte[payload.limit()];
            payload.get(bytes);
            List<Object> tuple = null;
            try {
                tuple = kafkaMsgDecoder.generateTuple(bytes);
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (tuple == null || tuple.size() != kafkaMsgDecoder.generateFields().size()) {
                //TODO 错误消息打印
            } else {
                KafkaConsumerMsgId messageId = new KafkaConsumerMsgId(config.getConsumerGroup(), config.getTopic(), partition, currentOffset);
                spoutOutputCollector.emit(tuple, messageId);
                topologyIdleTupleNum.decrementAndGet();
                // 发送的消息放到缓存，ack删除，fail重发
                emittedCache.put(new StringBuilder().append(partition).append('-').append(currentOffset).toString(), tuple);
            }
            break;
        }
    }


    /**
     * 拓扑消息成功处理
     *
     * @param messageId 消息ID对象
     */
    public void ack(KafkaConsumerMsgId messageId) {
        deleteAckData(messageId);
        deleteAckedOffsetZnodeFormZk(messageId);
    }

    /**
     * 删除缓存tuple
     *
     * @param messageId 消息ID对象
     */
    private void deleteAckData(KafkaConsumerMsgId messageId) {
        String key = new StringBuilder().append(messageId.getPartition()).append('-').append(messageId.getOffset()).toString();
        emittedCache.remove(key);
    }

    /**
     * 在zookeeper中建立已经发射，但是没有ack的消息节点
     *
     * @param messageId
     */
    public void createUnAckedOffsetZnodeToZk(KafkaConsumerMsgId messageId) {
        String path = new StringBuilder(zkKafkaConsumerGroupRootPath).append('/')
                .append(messageId.getGroup()).append("/offsets/")
                .append(messageId.getTopic()).append('/')
                .append(messageId.getPartition()).append("/")
                .append(messageId.getOffset()).toString();
        do {
            try {
                curatorFramework.create().creatingParentsIfNeeded().forPath(path);
                break;
            } catch (Exception e) {
                //连接失败 重新连接继续创建
                e.printStackTrace();
            }
        } while (true);
    }

    /**
     * 删除zookeeper中已经ack的消息节点
     *
     * @param messageId
     */
    public void deleteAckedOffsetZnodeFormZk(KafkaConsumerMsgId messageId) {
        String path = new StringBuilder(zkKafkaConsumerGroupRootPath).append('/')
                .append(messageId.getGroup()).append("/offsets/")
                .append(messageId.getTopic()).append('/')
                .append(messageId.getPartition()).append("/")
                .append(messageId.getOffset()).toString();
        do {
            try {
                curatorFramework.delete().forPath(path);
                break;
            } catch (KeeperException.NoNodeException nne) {
                break;
            } catch (Exception e) {
                //连接失败 重新连接继续创建
                e.printStackTrace();
            }
        } while (true);
    }


    /**
     * 拓扑消息失败处理
     *
     * @param messageId 消息ID对象
     */
    public void fail(KafkaConsumerMsgId messageId) {
        String key = new StringBuilder().append(messageId.getPartition()).append('-').append(messageId.getOffset()).toString();
        List<Object> tuple = emittedCache.get(key);
        if (tuple == null) {
            //如果 tuple为空  需要重新从kfakaf获取数据
            tuple = getFailTuple(messageId.getPartition(), messageId.getOffset());
        }
        if (tuple == null) {
            //kafka中已经取不到该数据，打日志
        } else {
            emittedCache.put(key, tuple);
            spoutOutputCollector.emit(tuple, messageId);
        }
    }

    /**
     * 从kafka中获取失败的tuple
     *
     * @param partition 分区
     * @param offset    偏移量
     * @return tuple
     */

    private List<Object> getFailTuple(int partition, long offset) {
        FetchRequestBuilder fetchRequestBuilder = new FetchRequestBuilder();
        SimpleConsumer consumer = null;
        do {
            if (consumer == null) {
                String topicPartitionLeaderBrokerId = getTopicPartitionLeaderBrokerId(partition);
                KafkaBroker kafkaBroker = getKafkaBroker(topicPartitionLeaderBrokerId);
                String clientId = generateSimpleConsumerClientId();
                try {
                    consumer = new SimpleConsumer(kafkaBroker.getHost(), kafkaBroker.getPort(), 100000, 64 * 1024, clientId);
                } catch (Exception e) {
                    continue;
                }
                fetchRequestBuilder.clientId(clientId);
            }
            FetchRequest req = fetchRequestBuilder.addFetch(config.getTopic(), partition, offset, 10000).build();
            FetchResponse fetchResponse = consumer.fetch(req);
            if (fetchResponse.hasError()) {
                if (fetchResponse.errorCode(config.getTopic(), partition) == ErrorMapping.OffsetOutOfRangeCode()) {
                    return null;
                } else {
                    consumer.close();
                    consumer = null;
                }
            } else {
                Iterator<MessageAndOffset> iterator = fetchResponse.messageSet(config.getTopic(), partition).iterator();
                while (iterator.hasNext()) {
                    MessageAndOffset messageAndOffset = iterator.next();
                    ByteBuffer payload = messageAndOffset.message().payload();
                    byte[] bytes = new byte[payload.limit()];
                    payload.get(bytes);
                    List<Object> tuple = null;
                    try {
                        tuple = kafkaMsgDecoder.generateTuple(bytes);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    if (tuple == null || tuple.size() != kafkaMsgDecoder.generateFields().size()) {
                        return null;
                    } else {
                        return tuple;
                    }
                }
                consumer.close();
            }
        } while (true);
    }


    /**
     * 生成消费者客户端ID
     *
     * @return
     */
    private String generateSimpleConsumerClientId() {
        String localHostAddress = null;
        try {
            localHostAddress = InetAddress.getLocalHost().toString();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return new StringBuilder(localHostAddress).append('-').append(UUID.randomUUID()).toString();
    }

    public Map<String, List<Object>> getEmittedCache() {
        return emittedCache;
    }
}
