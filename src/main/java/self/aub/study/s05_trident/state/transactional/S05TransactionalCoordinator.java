package self.aub.study.s05_trident.state.transactional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import self.aub.study.s05_trident.state.S05BasiTridentMetadata;
import storm.trident.spout.ITridentSpout;

/**
 * @author liujinxin
 * @since 2015-07-19 14:49
 */
public class S05TransactionalCoordinator implements ITridentSpout.BatchCoordinator<S05BasiTridentMetadata> {
    private static final Logger LOG = LoggerFactory.getLogger(S05TransactionalCoordinator.class);
    private int currentIndex;

    @Override
    public S05BasiTridentMetadata initializeTransaction(long txid, S05BasiTridentMetadata prevMetadata, S05BasiTridentMetadata currMetadata) {
        //LOG.warn("\n====>S05TransactionalCoordinator:{}\n====>prevMetadata:{}\n====>currMetadata:{}", this, prevMetadata, currMetadata);
        if (currMetadata != null) {
            return currMetadata;
        }
        return new S05BasiTridentMetadata(++currentIndex);
    }

    @Override
    public void success(long txid) {

    }

    @Override
    public boolean isReady(long txid) {
        return true;
    }

    @Override
    public void close() {

    }
}
