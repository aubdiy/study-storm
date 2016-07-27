package self.aub.study.s05_trident.aggregator;

import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

/**
 * @author liujinxin
 * @since 2015-07-17 08:29
 */
public class S05CountCombinerAggregator implements CombinerAggregator<Long> {


    @Override
    public Long init(TridentTuple tridentTuple) {
        return 1l;
    }

    @Override
    public Long combine(Long aLong, Long t1) {
        return aLong + t1;
    }

    @Override
    public Long zero() {
        return 0l;
    }
}
