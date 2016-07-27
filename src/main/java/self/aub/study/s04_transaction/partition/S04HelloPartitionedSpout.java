package self.aub.study.s04_transaction.partition;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BasePartitionedTransactionalSpout;
import backtype.storm.topology.base.BaseTransactionalSpout;
import backtype.storm.transactional.partitioned.IPartitionedTransactionalSpout;
import backtype.storm.tuple.Fields;
import self.aub.study.s04_transaction.S04HelloTransactionMetadata;
import self.aub.study.s04_transaction.S04HelloTransactionalSpoutCoordinator;
import self.aub.study.s04_transaction.S04HelloTransactionalSpoutEmitter;

import java.util.Map;

/**
 * @author liujinxin
 * @since 2015-07-13 21:20
 */
public class S04HelloPartitionedSpout extends BasePartitionedTransactionalSpout<S04HelloTransactionMetadata> {

    @Override
    public Coordinator getCoordinator(Map map, TopologyContext topologyContext) {
        return new S04HelloPartitionedSpoutCoordinator();
    }

    @Override
    public Emitter<S04HelloTransactionMetadata> getEmitter(Map map, TopologyContext topologyContext) {
        return new S04HelloPartitionedSpoutEmitter();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("transactionAttempt", "message"));

    }
}
