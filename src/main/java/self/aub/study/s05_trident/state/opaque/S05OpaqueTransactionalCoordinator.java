package self.aub.study.s05_trident.state.opaque;

import self.aub.study.s05_trident.state.partitioned.S05PartitionObj;
import self.aub.study.s05_trident.state.partitioned.S05PartitionObjs;
import storm.trident.spout.IOpaquePartitionedTridentSpout;

import java.util.ArrayList;
import java.util.List;

/**
 * @author liujinxin
 * @since 2015-07-20 14:43
 */
public class S05OpaqueTransactionalCoordinator implements IOpaquePartitionedTridentSpout.Coordinator<S05PartitionObjs> {


    @Override
    public boolean isReady(long l) {
        return true;
    }

    @Override
    public S05PartitionObjs getPartitionsForBatch() {
        List<S05PartitionObj> list = new ArrayList<>();
        list.add(new S05PartitionObj("partition-a"));
        list.add(new S05PartitionObj("partition-b"));
//        list.add(new S05Partition("partition-c"));
        return new S05PartitionObjs(list);
    }

    @Override
    public void close() {

    }
}
