package self.aub.study.chapter_3;

import storm.trident.state.OpaqueValue;
import storm.trident.state.map.IBackingMap;
import storm.trident.state.map.OpaqueMap;

/**
 * @author liujinxin
 * @since 2015-06-30 10:30
 */
public class TT extends OpaqueMap<Long> {
    protected TT(IBackingMap<OpaqueValue> backing) {
        super(backing);
    }
}
