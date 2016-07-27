package self.aub.study.s04_transaction;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;


/**
 * @author liujinxin
 * @since 2015-07-13 21:22
 */
public class S04HelloSplitBolt extends BaseBasicBolt {
    private static final Logger LOG = LoggerFactory.getLogger(S04HelloSplitBolt.class);


    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String message = tuple.getStringByField("message");
        String[] messageArr = message.split(",");
        TransactionAttempt tx = (TransactionAttempt) tuple.getValueByField("transactionAttempt");

//        final Random rand = new Random();
//        if (rand.nextInt(5) % 5 == 1) {
//            LOG.info("blot split ========>> fail transactionAttempt:{},{}  value:{} ", tx.getTransactionId(), tx.getAttemptId(), message);
//            throw new FailedException("出错了！！！！！！");
//        }
//        LOG.info("blot split ========>>transactionAttempt:{},{}  value:{} ", tx.getTransactionId(), tx.getAttemptId(), message);
        for (String word : messageArr) {
            basicOutputCollector.emit(new Values(tx, word));
        }

    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("transactionAttempt", "word"));
    }
}