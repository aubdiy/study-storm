package self.aub.study.s05_trident.state.opaque;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import self.aub.study.s05_trident.state.partitioned.S05PartitionObj;
import self.aub.study.s05_trident.state.partitioned.S05PartitionObjs;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IOpaquePartitionedTridentSpout;
import storm.trident.topology.TransactionAttempt;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author liujinxin
 * @since 2015-07-20 14:41
 */
public class S05OpaqueTransactionalEmitter implements IOpaquePartitionedTridentSpout.Emitter<S05PartitionObjs, S05PartitionObj, Map<String, Long>> {
    private static final Logger LOG = LoggerFactory.getLogger(S05OpaqueTransactionalEmitter.class);
    private static final String KEY_INDEX = "index";
    private String taskId;
    private int taskIndex;
    private String emiterInfo;
    private String[] wordArr = new String[]
            {
                    "上海",
                    "北京",
                    "北京",
                    "广州",
                    "深圳",
                    "天津",
                    "杭州",
                    "北京",
                    "沈阳",
                    "长春",
                    "苏州",
                    "济南"
            };
    private long txid = 0;

    public S05OpaqueTransactionalEmitter(TopologyContext topologyContext) {
        this.taskIndex = topologyContext.getThisTaskIndex();
        this.taskId = String.valueOf(topologyContext.getThisTaskId());
        int totalTasks = topologyContext.getComponentTasks(topologyContext.getThisComponentId()).size();
        this.emiterInfo = new StringBuilder().append(++taskIndex).append('/').append(totalTasks).toString();
    }

    @Override
    public Map<String, Long> emitPartitionBatch(TransactionAttempt transactionAttempt, TridentCollector tridentCollector,
                                                S05PartitionObj partition, Map<String, Long> map) {
        //if(taskIndex % 2 == 0){
        //    Utils.sleep(10000);
        //}
        Utils.sleep(3000);
        long data;
        if (map == null) {
            map = new HashMap<>();
            data = 0;
            LOG.warn("new");
        } else {
            LOG.warn("retry");
            data = map.get(KEY_INDEX);
        }

        int index1 = (int) (data % wordArr.length);
        int index2 = (index1 + 1) % wordArr.length;
        int index3 = (index2 + 1) % wordArr.length;
        String data1 = wordArr[index1];
        String data2 = wordArr[index2];
        String data3 = wordArr[index3];
        tridentCollector.emit(new Values(transactionAttempt, data1, index1));
        tridentCollector.emit(new Values(transactionAttempt, data2, index2));
        tridentCollector.emit(new Values(transactionAttempt, data3, index3));
        LOG.warn("trident emitter ========>>partition:{} transactionAttempt:{},{} index:{}\nvalue:{}\nvalue:{}\nvalue:{} ",
                partition.getId(),
                transactionAttempt.getTransactionId(), transactionAttempt.getAttemptId(),
                data, data1, data2, data3);
        map.put(KEY_INDEX, transactionAttempt.getTransactionId());
        txid = transactionAttempt.getTransactionId();
        return map;
    }


    @Override
    public List<S05PartitionObj> getOrderedPartitions(S05PartitionObjs s05PartitionObjs) {
        LOG.warn("############################### getOrderedPartitions, txid: {}", txid);
        return s05PartitionObjs.getList();
    }

    @Override
    public void refreshPartitions(List<S05PartitionObj> list) {
        LOG.warn("############################### refreshPartitions,txid: {}", txid);
    }

    @Override
    public void close() {

    }
}
