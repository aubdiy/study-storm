package self.aub.study.s03_group;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author liujinxin
 * @since 2015-07-13 14:41
 */
public class S03HelloGroupBolt extends BaseBasicBolt {
    private static final Logger LOG = LoggerFactory.getLogger(S03HelloGroupBolt.class);
    private String taskId;
    private String boltInfo;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        int taskIndex = context.getThisTaskIndex();
        int totalTasks = context.getComponentTasks(context.getThisComponentId()).size();
        this.taskId = String.valueOf(context.getThisTaskId());
        this.boltInfo = new StringBuilder().append(++taskIndex).append('/').append(totalTasks).toString();
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        LOG.info("blot: recive ========>>  taskId:{} info:{}  value:{}", taskId, boltInfo, tuple.getString(0));
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}

