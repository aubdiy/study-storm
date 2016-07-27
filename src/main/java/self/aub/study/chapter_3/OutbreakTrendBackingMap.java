package self.aub.study.chapter_3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.state.map.IBackingMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author liujinxin
 * @since 2015-06-07 14:14
 */
public class OutbreakTrendBackingMap implements IBackingMap<Long> {
    private static final Logger LOG = LoggerFactory.getLogger(OutbreakTrendBackingMap.class);
    private Map<String, Long> storage = new ConcurrentHashMap<>();

    @Override
    public List<Long> multiGet(List<List<Object>> list) {
        List<Long> values = new ArrayList<>();
        for (List<Object> key : list) {
            Long value = storage.get(key.get(0));
            if (value == null) {
                values.add(new Long(0));
            } else {
                values.add(value);
            }
        }
        return values;
    }

    @Override
    public void multiPut(List<List<Object>> list, List<Long> list1) {
        for (int i = 0; i < list.size(); i++) {
            LOG.info("Persisting [{}] ==>[{}]", list.get(i).get(0), list1.get(0));
            storage.put((String) list.get(i).get(0), list1.get(i));
        }
    }
}
