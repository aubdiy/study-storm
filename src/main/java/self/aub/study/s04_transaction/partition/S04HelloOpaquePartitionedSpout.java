package self.aub.study.s04_transaction.partition;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.transactional.partitioned.IOpaquePartitionedTransactionalSpout;
import backtype.storm.tuple.Fields;
import self.aub.study.s04_transaction.S04HelloTransactionMetadata;

import java.util.Map;

/**
 * @author liujinxin
 * @since 2015-07-14 16:05
 */
public class S04HelloOpaquePartitionedSpout implements IOpaquePartitionedTransactionalSpout<S04HelloTransactionMetadata> {
    @Override
    public Emitter<S04HelloTransactionMetadata> getEmitter(Map map, TopologyContext topologyContext) {
        return new S04HelloOpaquePartitionedEmitter();
    }

    @Override
    public Coordinator getCoordinator(Map map, TopologyContext topologyContext) {

        return new S04HelloOpaquePartitionedCoordinator();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("transactionAttempt", "message"));

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
