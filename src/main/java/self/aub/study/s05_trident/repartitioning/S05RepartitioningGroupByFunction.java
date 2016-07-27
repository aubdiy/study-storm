package self.aub.study.s05_trident.repartitioning;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Map;

/**
 * @author liujinxin
 * @since 2015-07-16 15:19
 */
public class S05RepartitioningGroupByFunction extends BaseFunction {
    private static final Logger LOG = LoggerFactory.getLogger(S05RepartitioningGroupByFunction.class);
    private String info;

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        super.prepare(conf, context);
        int partitions = context.numPartitions();
        int partitionsIndex = context.getPartitionIndex();
        info = new StringBuilder().append(partitionsIndex).append('/').append(partitions).toString();
    }


    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append('[');
        for (String field : tridentTuple.getFields()) {
            stringBuilder.append(field).append(':').append(tridentTuple.getValueByField(field)).append('|');
        }
        stringBuilder.setLength(stringBuilder.length() - 1);
        stringBuilder.append(']');
        LOG.info("print filter ==>> info:{} value:{} ", info, stringBuilder.toString());
    }
}