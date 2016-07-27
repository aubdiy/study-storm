package self.aub.study.s05_trident.state.opaque;

import backtype.storm.task.IMetricsContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.state.OpaqueValue;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.map.IBackingMap;
import storm.trident.state.map.OpaqueMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author liujinxin
 * @since 2015-07-20 22:06
 */
public class S05OpaqueTransactionlStateFactory implements StateFactory {
    private static final Logger LOG = LoggerFactory.getLogger(S05OpaqueTransactionlStateFactory.class);


    @Override
    public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        return new S05OpaqueTransactionalState(new S05OpaqueTransactionalBackingMap());
    }

    public static class S05OpaqueTransactionalState extends OpaqueMap<Long> {
        protected S05OpaqueTransactionalState(IBackingMap<OpaqueValue> backing) {
            super(backing);
        }
    }

    public static class S05OpaqueTransactionalBackingMap implements IBackingMap<OpaqueValue> {


        @Override
        public List<OpaqueValue> multiGet(List<List<Object>> list) {
            List<OpaqueValue> values = new ArrayList<>(list.size());
            for(List<Object> data: list){
                values.add(null);
                LOG.warn("state  ========>> multiGet, data:{},{}", data.get(0), data.get(1));
            }
            LOG.warn("state  ========>> multiGet");
            return values;
        }


        @Override
        public void multiPut(List<List<Object>> list, List<OpaqueValue> list1) {
            for (OpaqueValue opaqueValue:list1){
                Long currTxid = opaqueValue.getCurrTxid();
                opaqueValue.update(null,opaqueValue.getCurr());
            }
            LOG.warn("state  ========>> multiPut");
        }
    }


}
