package self.aub.study.chapter_3;

import storm.trident.state.OpaqueValue;
import storm.trident.state.map.IBackingMap;

import java.util.List;

/**
 * @author liujinxin
 * @since 2015-06-30 10:35
 */
public class TTM implements IBackingMap<OpaqueValue> {
    @Override
    public List<OpaqueValue> multiGet(List<List<Object>> list) {
        return null;
    }

    @Override
    public void multiPut(List<List<Object>> list, List<OpaqueValue> list1) {
//        list1.get(0).

    }
}
