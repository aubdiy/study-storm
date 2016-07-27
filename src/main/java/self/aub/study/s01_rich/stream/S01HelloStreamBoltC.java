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
public class S01HelloStreamBoltC extends BaseBasicBolt {
    private static final Logger LOG = LoggerFactory.getLogger(S01HelloStreamBoltC.class);
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
        String sourceStreamId = tuple.getSourceStreamId();
        String city = tuple.getStringByField("city");
        String tag = tuple.getStringByField("tag");
        LOG.info("blot C recive ========>>  taskId:{} info:{} sourceStreamId:{}, city:{}, tag:{}", taskId, boltInfo,sourceStreamId, city, tag);
        String value = new StringBuilder(sourceStreamId).append("-").append(city).append('-').append(tag).toString();

//        basicOutputCollector.emit( new Values(value, "AA"));
        basicOutputCollector.emit(new Values(value, "AA"));
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare( new Fields("message", "tag"));
    }

}