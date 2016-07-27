package self.aub.study.s05_trident.state.transactional;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import self.aub.study.s05_trident.state.S05BasiTridentMetadata;
import self.aub.study.s05_trident.state.non.S05NonTransactionalSpout;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.ITridentSpout;
import storm.trident.topology.TransactionAttempt;

/**
 * @author liujinxin
 * @since 2015-07-19 15:15
 */
public class S05TransactionalEmitter implements ITridentSpout.Emitter<S05BasiTridentMetadata> {
    private static final Logger LOG = LoggerFactory.getLogger(S05NonTransactionalSpout.class);
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

    public S05TransactionalEmitter(TopologyContext topologyContext) {
        this.taskIndex = topologyContext.getThisTaskIndex();
        this.taskId = String.valueOf(topologyContext.getThisTaskId());
        int totalTasks = topologyContext.getComponentTasks(topologyContext.getThisComponentId()).size();
        this.emiterInfo = new StringBuilder().append(++taskIndex).append('/').append(totalTasks).toString();
    }

    @Override
    public void emitBatch(TransactionAttempt transactionAttempt, S05BasiTridentMetadata s05BasiTridentMetadata, TridentCollector tridentCollector) {
        if(taskIndex % 2 == 0){
            Utils.sleep(5000);
        }
        Utils.sleep(3000);
        LOG.warn("trident emitter s05BasiTridentMetadata========>> {} ", s05BasiTridentMetadata.toString());
        int index1 = (int) s05BasiTridentMetadata.getIndex() % wordArr.length;
        int index2 = (index1 + 1) % wordArr.length;
        int index3 = (index2 + 1) % wordArr.length;
        String data1 = wordArr[index1];
        String data2 = wordArr[index2];
        String data3 = wordArr[index3];
        tridentCollector.emit(new Values(transactionAttempt, data1, index1));
        tridentCollector.emit(new Values(transactionAttempt, data2, index2));
        tridentCollector.emit(new Values(transactionAttempt, data3, index3));
        LOG.warn("trident emitter ========>>emiterInfo:{} transactionAttempt:{},{} index:{}\nvalue:{}\nvalue:{}\nvalue:{} ",
                emiterInfo, transactionAttempt.getTransactionId(), transactionAttempt.getAttemptId(),
                s05BasiTridentMetadata.getIndex(), data1, data2, data3);
    }

    @Override
    public void success(TransactionAttempt transactionAttempt) {
        LOG.warn("trident success ========>> transactionAttempt:{},{} ",
                transactionAttempt.getTransactionId(), transactionAttempt.getAttemptId());

    }

    @Override
    public void close() {

    }
}
