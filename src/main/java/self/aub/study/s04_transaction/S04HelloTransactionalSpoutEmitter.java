package self.aub.study.s04_transaction;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.transactional.ITransactionalSpout;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.Random;

/**
 * @author liujinxin
 * @since 2015-07-13 20:40
 */
public class S04HelloTransactionalSpoutEmitter implements ITransactionalSpout.Emitter<S04HelloTransactionMetadata> {
    private static final Logger LOG = LoggerFactory.getLogger(S04HelloTransactionalSpoutEmitter.class);
    private static String[] wordArr = new String[]
            {
                    "cat,dog,chicken,cat,dog,apple",
                    "cat,dog,apple,banana",
                    "cat,cat,dog,dog",
                    "pig,hive,hadoop,hbase",
                    "sqoop,spark"
            };
    private TopologyContext topologyContext;

    public S04HelloTransactionalSpoutEmitter(TopologyContext topologyContext) {
        this.topologyContext = topologyContext;
    }


    @Override
    public void emitBatch(TransactionAttempt transactionAttempt, S04HelloTransactionMetadata s04HelloTransactionMetadata,
                          BatchOutputCollector batchOutputCollector) {
        int taskIndex = topologyContext.getThisTaskIndex();

        Utils.sleep(3000);
        int index1 = s04HelloTransactionMetadata.getIndex() % wordArr.length;
        int index2 = (index1 + 1) % wordArr.length;
        String data1 = wordArr[index1];
        String data2 = wordArr[index2];
        batchOutputCollector.emit(new Values(transactionAttempt, data1));
        batchOutputCollector.emit(new Values(transactionAttempt, data2));
        LOG.info("spout emitter ========>> transactionAttempt:{},{} index:{}\nvalue:{}\nvalue:{} ",
                transactionAttempt.getTransactionId(), transactionAttempt.getAttemptId(),
                s04HelloTransactionMetadata.getIndex(), data1, data2);
    }

/*

    // spout 并发度>1 时 多个task每次 emitbatch会发送相同的 事务id, 如果有任意一个task出现阻塞，会导致 BatchBolt和 CommiterBolt 阻塞
    @Override
    public void emitBatch(TransactionAttempt transactionAttempt, S04HelloTransactionMetadata s04HelloTransactionMetadata,
                          BatchOutputCollector batchOutputCollector) {
        int taskIndex = topologyContext.getThisTaskIndex();
        int ms;
        if (taskIndex == 0) {
            ms = 3000;
            Utils.sleep(ms);
            int index1 = s04HelloTransactionMetadata.getIndex() % wordArr.length;
            int index2 = (index1 + 1) % wordArr.length;
            String data1 = wordArr[index1];
            String data2 = wordArr[index2];
            batchOutputCollector.emit(new Values(transactionAttempt, data1));
            batchOutputCollector.emit(new Values(transactionAttempt, data2));
            LOG.info("spout emitter ========>> transactionAttempt:{},{} index:{}\nvalue:{}\nvalue:{} ",
                    transactionAttempt.getTransactionId(), transactionAttempt.getAttemptId(),
                    s04HelloTransactionMetadata.getIndex(), data1, data2);
        } else {
            ms = 9000;
            Utils.sleep(ms);
            LOG.info("spout emitter ========>> transactionAttempt:{},{} index:{}\nvalue:{}\nvalue:{} ",
                    transactionAttempt.getTransactionId(), transactionAttempt.getAttemptId(),
                    s04HelloTransactionMetadata.getIndex(), null, null);
        }
    }
*/

    @Override
    public void cleanupBefore(BigInteger bigInteger) {

    }

    @Override
    public void close() {

    }
}
