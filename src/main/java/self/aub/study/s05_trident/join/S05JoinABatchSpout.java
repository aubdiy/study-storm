package self.aub.study.s05_trident.join;

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
public class S05JoinABatchSpout implements IBatchSpout {
    private static final Logger LOG = LoggerFactory.getLogger(S05JoinABatchSpout.class);
    private String[] wordArr = new String[]
            {
                    "A-北京",
                    "A-杭州,A-北京",
                    "A-北京,A-上海,A-杭州,A-杭州",
                    "A-沈阳,A-长春,A-哈尔滨",
                    "A-苏州,A-济南",
                    "A-未知"
            };
    private Fields fileds = new Fields("batch_id_A", "city", "index");
    private String taskId;
    private String info;

    @Override
    public void open(Map map, TopologyContext topologyContext) {
        int taskIndex = topologyContext.getThisTaskIndex();
        int totalTasks = topologyContext.getComponentTasks(topologyContext.getThisComponentId()).size();
        this.taskId = String.valueOf(topologyContext.getThisTaskId());
        this.info = new StringBuilder().append(++taskIndex).append('/').append(totalTasks).toString();
    }

    @Override
    public void emitBatch(long l, TridentCollector tridentCollector) {
        Utils.sleep(3000);
        int index1 = (int) l % wordArr.length;
        int index2 = (index1 + 1) % wordArr.length;
        String data1 = wordArr[index1];
        String data2 = wordArr[index2];
        tridentCollector.emit(new Values(l, data1, index1));
        tridentCollector.emit(new Values(l, data2, index2));
        LOG.info("spout emitter ========>>  taskId:{} info:{} batchid:{}\nvalue:{}\nvalue:{} ",taskId,info, l, data1, data2);
//        LOG.info("spout emitter ========>> batchid:{}\nvalue:{}\nvalue:{} ", l, index1, index2);

    }

    @Override
    public void ack(long l) {
        LOG.info("spout ack  ========>> batchid:{}", l);

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
