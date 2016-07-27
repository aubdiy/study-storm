package self.aub.study.s05_trident.state.partitioned;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import self.aub.study.s05_trident.state.S05BasiTridentMetadata;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IPartitionedTridentSpout;
import storm.trident.topology.TransactionAttempt;

import java.util.List;

/**
 * @author liujinxin
 * @since 2015-07-19 23:10
 */
public class S05PartitionedTransactionalEmitter implements IPartitionedTridentSpout.Emitter<S05PartitionObjs, S05PartitionObj, S05BasiTridentMetadata> {
    private static final Logger LOG = LoggerFactory.getLogger(S05PartitionedTransactionalEmitter.class);
    private String taskId;
    private int taskIndex;
    private String emiterInfo;
    private String[] wordArr = new String[]
            {
                    "北京",
                    "北京",
                    "上海",
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
    private int currentIndex;

    public S05PartitionedTransactionalEmitter(TopologyContext topologyContext) {
        this.taskIndex = topologyContext.getThisTaskIndex();
        this.taskId = String.valueOf(topologyContext.getThisTaskId());
        int totalTasks = topologyContext.getComponentTasks(topologyContext.getThisComponentId()).size();
        this.emiterInfo = new StringBuilder().append(++taskIndex).append('/').append(totalTasks).toString();
    }

    @Override
    public List<S05PartitionObj> getOrderedPartitions(S05PartitionObjs s05PartitionObjs) {
        return s05PartitionObjs.getList();
    }

    @Override
    public S05BasiTridentMetadata emitPartitionBatchNew(TransactionAttempt transactionAttempt, TridentCollector tridentCollector,
                                                        S05PartitionObj s05PartitionObj, S05BasiTridentMetadata s05BasiTridentMetadata) {
        s05BasiTridentMetadata = new S05BasiTridentMetadata(++currentIndex);
        if(taskIndex % 2 == 0){
            Utils.sleep(10000);
        }
        Utils.sleep(3000);
        int index1 = (int) s05BasiTridentMetadata.getIndex() % wordArr.length;
        int index2 = (index1 + 1) % wordArr.length;
        String data1 = wordArr[index1];
        String data2 = wordArr[index2];
        tridentCollector.emit(new Values(transactionAttempt, data1, index1));
        tridentCollector.emit(new Values(transactionAttempt, data2, index2));
        LOG.warn("trident emitter new ========>>emiterInfo:{}, s05PartitionObj:{} transactionAttempt:{},{} index:{}\nvalue:{}\nvalue:{} ",
                emiterInfo, s05PartitionObj.getId(),
                transactionAttempt.getTransactionId(), transactionAttempt.getAttemptId(),
                s05BasiTridentMetadata.getIndex(), data1, data2);

        return s05BasiTridentMetadata;
    }


    @Override
    public void emitPartitionBatch(TransactionAttempt transactionAttempt, TridentCollector tridentCollector,
                                   S05PartitionObj s05PartitionObj, S05BasiTridentMetadata s05BasiTridentMetadata) {
        Utils.sleep(3000);
        int index1 = (int) s05BasiTridentMetadata.getIndex() % wordArr.length;
        int index2 = (index1 + 1) % wordArr.length;
        String data1 = wordArr[index1];
        String data2 = wordArr[index2];
        tridentCollector.emit(new Values(transactionAttempt, data1, index1));
        tridentCollector.emit(new Values(transactionAttempt, data2, index2));
        LOG.warn("trident emitter retry ========>>emiterInfo:{}, s05PartitionObj:{} transactionAttempt:{},{} index:{}\nvalue:{}\nvalue:{} ",
                emiterInfo, s05PartitionObj.getId(),
                transactionAttempt.getTransactionId(), transactionAttempt.getAttemptId(),
                s05BasiTridentMetadata.getIndex(), data1, data2);
    }


    @Override
    public void refreshPartitions(List<S05PartitionObj> list) {

    }

    @Override
    public void close() {

    }
}
