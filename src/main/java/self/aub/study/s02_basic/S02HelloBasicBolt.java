package self.aub.study.s02_basic;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * @author liujinxin
 * @since 2015-07-10 17:17
 */
public class S02HelloBasicBolt extends BaseBasicBolt {
    private static final Logger LOG = LoggerFactory.getLogger(S02HelloBasicBolt.class);


    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        LOG.info("blot recive ========>>  value:{} ", tuple.getString(0));
        final Random rand = new Random();
        if (rand.nextInt(5) % 5 == 1) {
            throw new FailedException("出错了！！！！！！");
        }
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
