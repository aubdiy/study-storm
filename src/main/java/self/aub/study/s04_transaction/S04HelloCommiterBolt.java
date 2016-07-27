package self.aub.study.s04_transaction;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseTransactionalBolt;
import backtype.storm.transactional.ICommitter;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

/**
 * @author liujinxin
 * @since 2015-07-13 23:54
 */
public class S04HelloCommiterBolt extends BaseTransactionalBolt implements ICommitter {
    private static final Logger LOG = LoggerFactory.getLogger(S04HelloCommiterBolt.class);
    private TransactionAttempt transactionAttempt;
    private BatchOutputCollector batchOutputCollector;
    private Map<String, Integer> wordCountMap;
    private int msgWordTotalCount = 0;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, BatchOutputCollector batchOutputCollector, TransactionAttempt transactionAttempt) {
        this.transactionAttempt = transactionAttempt;
        this.batchOutputCollector = batchOutputCollector;
        this.wordCountMap = new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {
        msgWordTotalCount += tuple.getIntegerByField("count");
        wordCountMap.put(tuple.getStringByField("word"), tuple.getIntegerByField("count"));

    }

    @Override
    public void finishBatch() {
        BigInteger transactionId = transactionAttempt.getTransactionId();
        long attemptId = transactionAttempt.getAttemptId();
        LOG.info("commiter bolt  ========>>  transactionAttempt:{},{} ", transactionId, attemptId);
        for (Map.Entry<String, Integer> entry : wordCountMap.entrySet()) {
            String word = entry.getKey();
            Integer count = entry.getValue();
            LOG.info("commiter bolt  ========>>  word:{} count:{}, msgWordTotalCount:{}", word, count, msgWordTotalCount);
        }
        LOG.info("commiter bolt  ========>>  transactionAttempt:{},{}  OK! {}", transactionId, attemptId, this.hashCode());

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
