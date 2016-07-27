package self.aub.study.s03_group;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * @author liujinxin
 * @since 2015-07-10 17:17
 */
public class S03HelloDirectGroupSpout extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(S03HelloDirectGroupSpout.class);
    private SpoutOutputCollector collector;
    private List<Integer> taskList;
    private int numCounterTasks;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        this.taskList = topologyContext.getComponentTasks("hello");
        this.numCounterTasks = taskList.size();
    }

    @Override
    public void nextTuple() {
        Utils.sleep(3000);
        final String[] citys = new String[]{"北京", "深圳", "上海", "未知。。"};
        final Random rand = new Random();
        String city = citys[rand.nextInt(citys.length)];
        int taskId = getTaskId(city);
        collector.emitDirect(taskId, new Values(city));
        LOG.info("spout emit ========>>taskId:{}  city:{} ", taskId, city);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(true, new Fields("city"));
    }

    public int getTaskId(String city) {
        int index = 0;
        if (!city.isEmpty()) {
            index = city.charAt(0) % numCounterTasks;
        }
        return taskList.get(index);
    }
}
