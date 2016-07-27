package self.aub.study.s05_trident.state.opaque;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import self.aub.study.s05_trident.state.partitioned.S05PartitionObj;
import self.aub.study.s05_trident.state.partitioned.S05PartitionObjs;
import storm.trident.spout.IOpaquePartitionedTridentSpout;

import java.util.Map;

/**
 * @author liujinxin
 * @since 2015-07-17 16:39
 */
public class S05OpaqueTransactionalSpout implements IOpaquePartitionedTridentSpout<S05PartitionObjs,S05PartitionObj,Map<String,Long>> {


    @Override
    public Emitter<S05PartitionObjs, S05PartitionObj, Map<String,Long>> getEmitter(Map map, TopologyContext topologyContext) {
        return new S05OpaqueTransactionalEmitter(topologyContext);
    }

    @Override
    public Coordinator getCoordinator(Map map, TopologyContext topologyContext) {
        return new S05OpaqueTransactionalCoordinator();
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
