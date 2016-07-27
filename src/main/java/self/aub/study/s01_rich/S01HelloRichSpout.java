package self.aub.study.s01_rich;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;

/**
 * @author liujinxin
 * @since 2015-07-10 17:17
 */
public class S01HelloRichSpout extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(S01HelloRichSpout.class);
    private SpoutOutputCollector collector;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        Utils.sleep(5000);
        final String[] citys = new String[]{"北京", "上海", "广州", "深圳", "未知..."};
        final Random rand = new Random();
        String city = citys[rand.nextInt(citys.length)];
        collector.emit(new Values(city));
        LOG.info("spout emit ========>>  city:{} ", city);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("city"));
    }


}
