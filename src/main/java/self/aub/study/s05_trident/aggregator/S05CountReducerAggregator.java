package self.aub.study.s05_trident.aggregator;

import storm.trident.operation.ReducerAggregator;
import storm.trident.tuple.TridentTuple;

/**
 * @author liujinxin
 * @since 2015-07-17 08:29
 */
public class S05CountReducerAggregator implements ReducerAggregator<Long> {


    @Override
    public Long init() {
        return 0l;
    }

    @Override
    public Long reduce(Long aLong, TridentTuple tridentTuple) {
        return aLong +1;
    }
}
