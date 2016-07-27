package self.aub.study.s05_trident.state.non;

import backtype.storm.task.IMetricsContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.map.IBackingMap;
import storm.trident.state.map.NonTransactionalMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * @author liujinxin
 * @since 2015-07-20 21:34
 */

public class S05NonTransactionlStateFactory implements StateFactory {
    private static final Logger LOG = LoggerFactory.getLogger(S05NonTransactionlStateFactory.class);


    @Override
    public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        return new S05NonTransactionlState(new S05BackingMap());
    }


    public static class S05NonTransactionlState extends NonTransactionalMap<Long> {
        protected S05NonTransactionlState(IBackingMap<Long> backing) {
            super(backing);
        }
    }

    public static class S05BackingMap implements IBackingMap<Long> {

        @Override
        public List<Long> multiGet(List<List<Object>> list) {
            List<Long> values = new ArrayList<>(list.size());
            for(List<Object> data: list){
                values.add(0l);
            }
            LOG.info("state  ========>> multiGet");
            return values;
        }

        @Override
        public void multiPut(List<List<Object>> list, List<Long> list1) {
            LOG.info("state  ========>> multiPut");
        }
    }
}




