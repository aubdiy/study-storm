package self.aub.study.s05_trident.state.non;

import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.topology.TransactionAttempt;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;

/**
 * @author liujinxin
 * @since 2015-07-17 17:18
 */
public class S05BasicQuery extends BaseQueryFunction<S05BasicState, String> {
    private static final Logger LOG = LoggerFactory.getLogger(S05BasicQuery.class);
    private long l = 0;

    @Override
    public List<String> batchRetrieve(S05BasicState s05BasicState, List<TridentTuple> list) {

        TransactionAttempt ta = null;
        //long result = ((TransactionAttempt) list.get(0).getValueByField("tx")).getTransactionId();

        List<String> resultList = new ArrayList<>(list.size());
        for (TridentTuple tridentTuple : list) {
            //ta = (TransactionAttempt) tridentTuple.getValueByField("tx");
            //if ((result ^ ta.getTransactionId()) != 0l) {
            //    throw new RuntimeException("出错了！！！！！！");
            //}
            String city = tridentTuple.getStringByField("city");
            resultList.add(s05BasicState.getPopulation(city));
        }

//        final Random rand = new Random();
//        if (rand.nextInt(3) % 3 == 1) {
////        if (++l > 4) {
//            LOG.info("query  ========>> fail transactionAttempt:{},{}  count:{} ", ta.getTransactionId(), ta.getAttemptId(), resultList.size());
//            throw new FailedException("出错了！！！！！！");
//        }
//        LOG.info("query ========>>transactionAttempt:{},{}  count:{} ", ta.getTransactionId(), ta.getAttemptId(), resultList.size());
        //LOG.info("query ========>>transactionAttempt:{},{}  count:{} ", ta.getTransactionId(), ta.getAttemptId(), resultList.size());
        Utils.sleep(2000);


        return resultList;
    }

    @Override
    public void execute(TridentTuple tridentTuple, String s, TridentCollector tridentCollector) {
        tridentCollector.emit(new Values(s));
    }
}
