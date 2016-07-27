package self.aub.study.chapter_3;

import storm.trident.state.TransactionalValue;
import storm.trident.state.map.IBackingMap;
import storm.trident.state.map.TransactionalMap;

/**
 * @author liujinxin
 * @since 2015-06-30 10:24
 */
public class T extends TransactionalMap<Long> {
    protected T(IBackingMap<TransactionalValue> backing) {
        super(backing);
    }
}
