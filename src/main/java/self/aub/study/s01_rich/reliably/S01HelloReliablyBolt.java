package self.aub.study.s01_rich.reliably;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;

/**
 * @author liujinxin
 * @since 2015-07-10 17:17
 */
public class S01HelloReliablyBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(S01HelloReliablyBolt.class);
    private OutputCollector outputCollector;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        Utils.sleep(10000000);
        LOG.info("blot recive ========>>  value:{} ", tuple.getString(0));
        final Random rand = new Random();
        if(rand.nextInt(5) % 5 == 1){
            outputCollector.fail(tuple);
        }else{
            outputCollector.ack(tuple);

        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }
}
