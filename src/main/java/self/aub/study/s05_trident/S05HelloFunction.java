package self.aub.study.s05_trident;

import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Map;

/**
 * @author liujinxin
 * @since 2015-07-16 16:31
 */
public class S05HelloFunction extends BaseFunction {
    private static final Logger LOG = LoggerFactory.getLogger(S05HelloFunction.class);
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
        String data = tridentTuple.getString(1);
        int count = data.split(",").length;
        /** 如果function 没有输出，将导致这个调数据被过滤掉 **/
        tridentCollector.emit(new Values(count));
        LOG.info("function ==>> partitions info:{} count:[{}]", info, count);
    }
}
