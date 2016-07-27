package self.aub.study.chapter_3;

import storm.trident.state.map.IBackingMap;
import storm.trident.state.map.NonTransactionalMap;

/**
 * @author liujinxin
 * @since 2015-06-07 14:37
 */
public class OutbreakTrendState extends NonTransactionalMap<Long> {
    protected OutbreakTrendState(OutbreakTrendBackingMap outbreakTrendBackingMap) {
        super(outbreakTrendBackingMap);
    }
}
