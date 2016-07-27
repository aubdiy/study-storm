package self.aub.study.s05_trident.aggregator;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;

import java.util.Map;

/**
 * @author liujinxin
 * @since 2015-07-15 23:56
 */
public class    S05AggregatorBatchSpout implements IBatchSpout {
    private static final Logger LOG = LoggerFactory.getLogger(S05AggregatorBatchSpout.class);
    private String[] wordArr = new String[]
            {
                    "北京,北京,上海,广州,深圳,天津",
                    "杭州,北京",
                    "北京,上海,杭州,杭州",
                    "沈阳,长春,哈尔滨",
                    "苏州,济南"
            };
    private Fields fileds = new Fields("batch_id","city", "index");

    @Override
    public void open(Map map, TopologyContext topologyContext) {

    }

    @Override
    public void emitBatch(long l, TridentCollector tridentCollector) {
        Utils.sleep(3000);
        int index1 = (int) l % wordArr.length;
        int index2 = (index1 + 1) % wordArr.length;
        int index3 = (index2 + 1) % wordArr.length;
        String data1 = wordArr[index1];
        String data2 = wordArr[index2];
        String data3 = wordArr[index3];
        tridentCollector.emit(new Values(l,data1, index1));
        tridentCollector.emit(new Values(l,data2, index2));
        tridentCollector.emit(new Values(l,data3, index3));
//        LOG.info("spout emitter ========>> batchid:{}\nvalue:{}\nvalue:{} ", l, data1, data2);
//        LOG.info("spout emitter ========>> batchid:{}\nvalue:{}\nvalue:{} ", l, index1, index2);

    }

    @Override
    public void ack(long l) {
//        LOG.info("spout ack  ========>> batchid:{}", l);

    }

    @Override
    public void close() {

    }

    @Override
    public Map getComponentConfiguration() {
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return fileds;
    }
}
