package self.aub.study.chapter_3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

/**
 * @author liujinxin
 * @since 2015-06-07 13:30
 */
public class DiseaseFilter extends BaseFilter {
    private static final long serialVersionUID = 1l;
    private static final Logger LOG = LoggerFactory.getLogger(DiseaseFilter.class);

    @Override
    public boolean isKeep(TridentTuple tridentTuple) {
        DiagnosisEvent diagnosis = (DiagnosisEvent) tridentTuple.get(0);
        Integer code = Integer.parseInt(diagnosis.diagnosisCode);
        if (code.intValue() <= 322) {
            LOG.debug("Emitting disease [{}]", diagnosis.diagnosisCode);
            return true;
        } else {
            LOG.debug("Filtering disease [{}]", diagnosis.diagnosisCode);
            return false;
        }

    }
}
