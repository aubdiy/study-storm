package self.aub.study.s05_trident;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Map;

/**
 * @author liujinxin
 * @since 2015-07-16 15:19
 */
public class S05HelloFilter extends BaseFilter {
    private static final Logger LOG = LoggerFactory.getLogger(S05HelloFilter.class);
    private String info;

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        super.prepare(conf, context);
        int partitions = context.numPartitions();
        int partitionsIndex = context.getPartitionIndex();
        info = new StringBuilder().append(partitionsIndex).append('/').append(partitions).toString();
    }

    @Override
    public boolean isKeep(TridentTuple tridentTuple) {

        LOG.info("filter ==>> partitions info:{} value:[{}]", info, tridentTuple.get(0));
        return !tridentTuple.getInteger(0).equals(5);
    }
}