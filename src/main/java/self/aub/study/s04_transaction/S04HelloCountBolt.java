package self.aub.study.s04_transaction;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBatchBolt;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

/**
 * @author liujinxin
 * @since 2015-07-13 21:34
 */
public class S04HelloCountBolt extends BaseBatchBolt {
    private static final Logger LOG = LoggerFactory.getLogger(S04HelloCountBolt.class);
    private TransactionAttempt transactionAttempt;
    private BatchOutputCollector collector;
    private Map<String, Integer> wordCountMap;
    private String boltInfo;
    private String taskId;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, BatchOutputCollector batchOutputCollector, Object o) {
        this.transactionAttempt = (TransactionAttempt) o;
        this.wordCountMap = new HashMap<>();
        this.collector = batchOutputCollector;
        int taskIndex = topologyContext.getThisTaskIndex();
        int totalTasks = topologyContext.getComponentTasks(topologyContext.getThisComponentId()).size();
        this.taskId = String.valueOf(topologyContext.getThisTaskId());
        this.boltInfo = new StringBuilder().append(++taskIndex).append('/').append(totalTasks).toString();
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        Integer count = wordCountMap.get(word);
        if (count == null) {
            count = 0;
        }
        wordCountMap.put(word, ++count);
//        LOG.info("blot count ========>>  taskId:{} info:{}  word:{}", taskId, boltInfo, word);


    }

    @Override
    public void finishBatch() {
        BigInteger transactionId = transactionAttempt.getTransactionId();
        long attemptId = transactionAttempt.getAttemptId();

        String baseLog = new StringBuilder("transactionAttempt:").append(transactionId).append(',')
                .append(attemptId).append(" taskId:").append(taskId).append(" info:").append(boltInfo).toString();

        LOG.info("blot count finishBatch start ==>> {}", baseLog);
        for (Map.Entry<String, Integer> entry : wordCountMap.entrySet()) {
            String word = entry.getKey();
            Integer count = entry.getValue();
            collector.emit(new Values(transactionAttempt, word, count));
//            LOG.info("blot count finishBatch  ==>> {} [word:{},count:{}] ", baseLog, word, count);
        }
        LOG.info("blot count finishBatch end  ==>> {}", baseLog);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("transactionAttempt", "word", "count"));
    }
}
