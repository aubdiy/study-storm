package self.aub.study.s04_transaction;

import backtype.storm.transactional.ITransactionalSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;

/**
 * @author liujinxin
 * @since 2015-07-13 20:42
 */
public class S04HelloTransactionalSpoutCoordinator implements ITransactionalSpout.Coordinator<S04HelloTransactionMetadata> {
    private static final Logger LOG = LoggerFactory.getLogger(S04HelloTransactionalSpoutCoordinator.class);
    private  int currentIndex;

    @Override
    public boolean isReady() {
        //初始化操作
        return true;
    }

    @Override
    public S04HelloTransactionMetadata initializeTransaction(BigInteger bigInteger, S04HelloTransactionMetadata s04HelloTransactionMetadata) {
        return new S04HelloTransactionMetadata(++currentIndex);
    }

    @Override
    public void close() {
        //关闭操作
    }

}
