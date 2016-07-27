package self.aub.study.chapter_3;

import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

/**
 * @author liujinxin
 * @since 2015-06-07 14:12
 */
public class Count implements CombinerAggregator<Long> {
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
