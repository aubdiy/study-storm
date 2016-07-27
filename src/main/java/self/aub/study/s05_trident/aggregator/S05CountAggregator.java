package self.aub.study.s05_trident.aggregator;

import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Map;

/**
 * @author liujinxin
 * @since 2015-07-17 10:35
 */
public class S05CountAggregator extends BaseAggregator<S05CountAggregator.CountState> {
    private static final Logger LOG = LoggerFactory.getLogger(S05CountAggregator.class);
    private String info;

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        super.prepare(conf, context);
        int partitions = context.numPartitions();
        int partitionsIndex = context.getPartitionIndex();
        info = new StringBuilder().append(partitionsIndex).append('/').append(partitions).toString();
    }

    public static class CountState {
        long count = 0;
    }


    public CountState init(Object batchId, TridentCollector collector) {
        return new CountState();
    }


    public void aggregate(CountState state, TridentTuple tuple, TridentCollector collector) {
        state.count += 1;
    }

    public void complete(CountState state, TridentCollector collector) {
        collector.emit(new Values(state.count));
        LOG.info("print filter ==>> info:{} value:{} ", info, state.count);
    }

}
