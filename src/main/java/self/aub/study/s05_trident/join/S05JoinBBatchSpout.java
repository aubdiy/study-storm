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
public class S05JoinBBatchSpout implements IBatchSpout {
    private static final Logger LOG = LoggerFactory.getLogger(S05JoinBBatchSpout.class);
    private String[] wordArr = new String[]
            {
                    "B-北京",
                    "B-杭州,B-北京",
                    "B-北京,B-上海,B-杭州,B-杭州",
                    "B-沈阳,B-长春,B-哈尔滨",
                    "B-苏州,B-济南",
                    "B-未知"
            };
    private Fields fileds = new Fields("batch_id_B", "city", "index");
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
        Utils.sleep(10000);
        int index1 = (int) l % wordArr.length;
        int index2 = (index1 + 1) % wordArr.length;
        String data1 = wordArr[index1];
        String data2 = wordArr[index2];
        tridentCollector.emit(new Values(l, data1, index1));
        tridentCollector.emit(new Values(l+1, data2, index2));
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
