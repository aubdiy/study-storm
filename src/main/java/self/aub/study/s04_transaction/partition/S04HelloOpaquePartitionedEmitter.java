package self.aub.study.s04_transaction.partition;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.transactional.partitioned.IOpaquePartitionedTransactionalSpout;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import self.aub.study.s04_transaction.S04HelloTransactionMetadata;

/**
 * @author liujinxin
 * @since 2015-07-14 16:06
 */
public class S04HelloOpaquePartitionedEmitter implements IOpaquePartitionedTransactionalSpout.Emitter<S04HelloTransactionMetadata> {

    private static final Logger LOG = LoggerFactory.getLogger(S04HelloPartitionedSpoutEmitter.class);
    private static String[] wordArr = new String[]
            {
                    "cat,dog,chicken,cat,dog,apple",
                    "cat,dog,apple,banana",
                    "cat,cat,dog,dog",
                    "pig,hive,hadoop,hbase",
                    "sqoop,spark"
            };

    private static int currentIndex = 0;

    @Override
    public S04HelloTransactionMetadata emitPartitionBatch(TransactionAttempt transactionAttempt,
                                                          BatchOutputCollector batchOutputCollector,
                                                          int i, S04HelloTransactionMetadata s04HelloTransactionMetadata) {
        Utils.sleep(3000);
        s04HelloTransactionMetadata = new S04HelloTransactionMetadata(++currentIndex);
        String data = wordArr[s04HelloTransactionMetadata.getIndex() % wordArr.length];
        batchOutputCollector.emit(new Values(transactionAttempt, data));
        LOG.info("spout emitter ========>>partition:{}  transactionAttempt:{},{}  index:{}  value:{} ",
                i, transactionAttempt.getTransactionId(), transactionAttempt.getAttemptId(),
                s04HelloTransactionMetadata.getIndex(), data);

        return s04HelloTransactionMetadata;
    }

    @Override
    public int numPartitions() {
        return 2;
    }

    @Override
    public void close() {

    }
}
