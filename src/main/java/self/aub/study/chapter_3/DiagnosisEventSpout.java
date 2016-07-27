package self.aub.study.chapter_3;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import storm.trident.spout.ITridentSpout;

import java.util.Map;

/**
 * @author liujinxin
 * @since 2015-06-07 12:35
 */
public class DiagnosisEventSpout implements ITridentSpout<Long> {
    private static final long serialVersionUID = 1l;
    private SpoutOutputCollector collector;
    private ITridentSpout.BatchCoordinator<Long> coordinator = new DefaultCoorDinator();
    private ITridentSpout.Emitter<Long> emitter = new DiagonsisEventEmitter();


    @Override
    public BatchCoordinator<Long> getCoordinator(String s, Map map, TopologyContext topologyContext) {
        return coordinator;
    }

    @Override
    public Emitter<Long> getEmitter(String s, Map map, TopologyContext topologyContext) {
        return emitter;
    }

    @Override
    public Map getComponentConfiguration() {
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("event");
    }
}
