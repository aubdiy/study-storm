package self.aub.study.s04_transaction.partition;

import backtype.storm.transactional.partitioned.IPartitionedTransactionalSpout;

/**
 * @author liujinxin
 * @since 2015-07-14 11:50
 */
public class S04HelloPartitionedSpoutCoordinator implements IPartitionedTransactionalSpout.Coordinator {
    @Override
    public int numPartitions() {
        return 2;
    }

    @Override
    public boolean isReady() {
        return true;
    }

    @Override
    public void close() {

    }
}
