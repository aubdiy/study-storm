package self.aub.study.s05_trident.state.non;

import backtype.storm.task.IMetricsContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import java.util.Map;

/**
 * @author liujinxin
 * @since 2015-07-17 17:01
 */
public class S05BasicStateFactory implements StateFactory {
    private static final Logger LOG = LoggerFactory.getLogger(S05BasicStateFactory.class);

    @Override
    public State makeState(Map map, IMetricsContext iMetricsContext, int partitionIndex, int numPartitions) {
        LOG.info("state factory ========>> partitionIndex:{} numPartitions:{}", partitionIndex, numPartitions);
        return new S05BasicState();
    }
}
