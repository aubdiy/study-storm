package self.aub.study.s05_trident.state.transactional;

import backtype.storm.task.IMetricsContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.TransactionalValue;
import storm.trident.state.map.IBackingMap;
import storm.trident.state.map.TransactionalMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author liujinxin
 * @since 2015-07-20 22:06
 */
public class S05TransactionalStateFactory implements StateFactory {
    private static final Logger LOG = LoggerFactory.getLogger(S05TransactionalStateFactory.class);


    @Override
    public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        return new S05TransactionalState(new S05TransactionalBackingMap());
    }

    public static class S05TransactionalState extends TransactionalMap<Long> {
        protected S05TransactionalState(IBackingMap<TransactionalValue> backing) {
            super(backing);
        }
    }

    public static class S05TransactionalBackingMap implements IBackingMap<TransactionalValue> {


        @Override
        public List<TransactionalValue> multiGet(List<List<Object>> list) {
            List<TransactionalValue> values = new ArrayList<>(list.size());
            for (List<Object> data : list) {
                values.add(null);
                LOG.warn("state  ========>> multiGet, data:{},{}", data.get(0), data.get(1));
            }
            LOG.warn("state  ========>> multiGet");
            return values;
        }

        @Override
        public void multiPut(List<List<Object>> list, List<TransactionalValue> list1) {
            LOG.warn("state  ========>> multiPut");
            //if (list1.get(0).getTxid() % 2 == 0) {
            //    try {
            //        Thread.sleep(10000);
            //    } catch (InterruptedException e) {
            //        e.printStackTrace();
            //    }
            //}
        }
    }


}
