package self.aub.study.s04_transaction.partition;

import backtype.storm.transactional.partitioned.IOpaquePartitionedTransactionalSpout;

/**
 * @author liujinxin
 * @since 2015-07-14 16:06
 */
public class S04HelloOpaquePartitionedCoordinator implements IOpaquePartitionedTransactionalSpout.Coordinator {
    @Override
    public boolean isReady() {
        return true;
    }

    @Override
    public void close() {

    }
}
