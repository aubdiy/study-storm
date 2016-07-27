package self.aub.study.chapter_3;

import storm.trident.state.TransactionalValue;
import storm.trident.state.map.IBackingMap;

import java.util.List;

/**
 * @author liujinxin
 * @since 2015-06-30 10:29
 */
public class TM implements IBackingMap<TransactionalValue> {
    @Override
    public List<TransactionalValue> multiGet(List<List<Object>> list) {
        return null;
    }

    @Override
    public void multiPut(List<List<Object>> list, List<TransactionalValue> list1) {
        list1.get(0).getTxid();
    }
}
