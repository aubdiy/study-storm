package self.aub.study.s05_trident.state.transactional;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import self.aub.study.s05_trident.state.S05BasiTridentMetadata;
import storm.trident.spout.ITridentSpout;

import java.util.Map;

/**
 * @author liujinxin
 * @since 2015-07-17 16:39
 */
public class S05TransactionalSpout implements ITridentSpout<S05BasiTridentMetadata> {


    @Override
    public BatchCoordinator<S05BasiTridentMetadata> getCoordinator(String txStateId, Map conf, TopologyContext context) {
        return new S05TransactionalCoordinator();
    }

    @Override
    public Emitter<S05BasiTridentMetadata> getEmitter(String txStateId, Map conf, TopologyContext context) {
        return new S05TransactionalEmitter(context);
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
