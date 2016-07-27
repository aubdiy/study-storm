package self.aub.study.s05_trident.state.partitioned;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.spout.IPartitionedTridentSpout;

import java.util.ArrayList;
import java.util.List;

/**
 * @author liujinxin
 * @since 2015-07-19 23:05
 */
public class S05PartitionedTransactionalCoordinator implements IPartitionedTridentSpout.Coordinator<S05PartitionObjs> {
    private static final Logger LOG = LoggerFactory.getLogger(S05PartitionedTransactionalCoordinator.class);
    @Override
    public S05PartitionObjs getPartitionsForBatch() {
        LOG.warn("===>getPartitionsForBatch:{}",this);
        List<S05PartitionObj> list = new ArrayList<>();
        list.add(new S05PartitionObj("partition-a"));
        list.add(new S05PartitionObj("partition-b"));
        list.add(new S05PartitionObj("partition-c"));
        return new S05PartitionObjs(list);
    }

    @Override
    public boolean isReady(long txid) {
        return true;
    }

    @Override
    public void close() {

    }
}
