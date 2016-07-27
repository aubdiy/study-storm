package self.aub.study.s04_transaction;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseTransactionalSpout;
import backtype.storm.tuple.Fields;

import java.util.Map;

/**
 * @author liujinxin
 * @since 2015-07-13 21:20
 */
public class S04HelloTransactionalSpout extends BaseTransactionalSpout<S04HelloTransactionMetadata>{
    @Override
    public Coordinator<S04HelloTransactionMetadata> getCoordinator(Map map, TopologyContext topologyContext) {
        return new S04HelloTransactionalSpoutCoordinator();
    }

    @Override
    public Emitter<S04HelloTransactionMetadata> getEmitter(Map map, TopologyContext topologyContext) {
        return new S04HelloTransactionalSpoutEmitter(topologyContext);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("transactionAttempt", "message"));

    }
}
