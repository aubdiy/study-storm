package self.aub.study.chapter_3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.spout.ITridentSpout;

import java.io.Serializable;

/**
 * @author liujinxin
 * @since 2015-06-07 12:40
 */
public class DefaultCoorDinator implements ITridentSpout.BatchCoordinator<Long>, Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultCoorDinator.class);

    @Override
    public Long initializeTransaction(long l, Long aLong, Long x1) {
        LOG.info("Initializing Transaction [{}, {}, {}]", l, aLong, x1);
        return null;
    }

    @Override
    public void success(long l) {
        LOG.info("Successful Transaction [{}]", l);
    }

    @Override
    public boolean isReady(long l) {
        return true;
    }

    @Override
    public void close() {

    }
}
