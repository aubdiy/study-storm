package self.aub.study.s01_rich.stream;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author liujinxin
 * @since 2015-07-14 16:53
 */
public class S01HelloStreamBoltB extends BaseBasicBolt {
    private static final Logger LOG = LoggerFactory.getLogger(S01HelloStreamBoltB.class);
    private String boltInfo;
    private String taskId;


    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        int taskIndex = context.getThisTaskIndex();
        int totalTasks = context.getComponentTasks(context.getThisComponentId()).size();
        this.taskId = String.valueOf(context.getThisTaskId());
        this.boltInfo = new StringBuilder().append(++taskIndex).append('/').append(totalTasks).toString();
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        LOG.info("blot B recive ========>>  taskId:{} info:{}  value:{}", taskId, boltInfo, tuple.getString(0));
        basicOutputCollector.emit("stream-B1", new Values(tuple.getString(0), "B1"));
        basicOutputCollector.emit("stream-B2", new Values(tuple.getString(0), "B2"));
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
//        outputFieldsDeclarer.declare(new Fields("city", "tag"));
        outputFieldsDeclarer.declareStream("stream-B1", new Fields("city", "tag"));
        outputFieldsDeclarer.declareStream("stream-B2", new Fields("city", "tag"));
    }

}