package self.aub.study.chapter_3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;

/**
 * @author liujinxin
 * @since 2015-06-07 13:57
 */
public class HourAssignment extends BaseFunction {
    private static final long serialVersionUID = 1l;
    private static final Logger LOG = LoggerFactory.getLogger(HourAssignment.class);

    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        DiagnosisEvent diagnosis = (DiagnosisEvent) tridentTuple.get(0);
        String city = (String) tridentTuple.get(1);
        long timestamp = diagnosis.time;
        long hourSinceEpoch = timestamp / 1000 / 60 / 60;
        LOG.debug("Key=[{}: {}]", city, hourSinceEpoch);
        String key = city + ":" + diagnosis.diagnosisCode + ":" + hourSinceEpoch;
        List<Object> values = new ArrayList<>();
        values.add(hourSinceEpoch);
        values.add(key);
        tridentCollector.emit(values);

    }
}
