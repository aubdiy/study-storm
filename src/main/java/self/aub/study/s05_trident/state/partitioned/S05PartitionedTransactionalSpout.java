package self.aub.study.s05_trident.state.partitioned;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import self.aub.study.s05_trident.state.S05BasiTridentMetadata;
import storm.trident.spout.IPartitionedTridentSpout;

import java.util.Map;

/**
 * @author liujinxin
 * @since 2015-07-17 16:39
 */
public class S05PartitionedTransactionalSpout implements IPartitionedTridentSpout<S05PartitionObjs,S05PartitionObj,S05BasiTridentMetadata> {


    @Override
    public Coordinator<S05PartitionObjs> getCoordinator(Map map, TopologyContext topologyContext) {
        return new S05PartitionedTransactionalCoordinator();
    }

    @Override
    public Emitter<S05PartitionObjs, S05PartitionObj, S05BasiTridentMetadata> getEmitter(Map map, TopologyContext topologyContext) {
        return new S05PartitionedTransactionalEmitter(topologyContext);
    }

    @Override
    public Map getComponentConfiguration() {
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("tx", "city", "index");
    }
}
