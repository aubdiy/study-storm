package self.aub.study.chapter_3;

import clojure.lang.Obj;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;

/**
 * @author liujinxin
 * @since 2015-06-07 14:03
 */
public class OutbreakDetector extends BaseFunction {

    private static final long serialVersionUID = 1l;
    public static final int THRESHOLD = 10000;

    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        String key = (String) tridentTuple.getValue(0);
        Long count = (Long) tridentTuple.getValue(1);
        if (count > THRESHOLD) {
            List<Object> values = new ArrayList<>();
            values.add("Outbreak detected for [" + key + "]!");
            tridentCollector.emit(values);
        }
    }
}
