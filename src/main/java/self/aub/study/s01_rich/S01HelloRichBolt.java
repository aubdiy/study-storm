package self.aub.study.s01_rich;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author liujinxin
 * @since 2015-07-10 17:17
 */
public class S01HelloRichBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(S01HelloRichBolt.class);

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    }

    @Override
    public void execute(Tuple tuple) {
        LOG.info("blot recive ========>>  city:{} ", tuple.getString(0));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }
}
