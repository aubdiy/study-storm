package self.aub.study.chapter_3;


import backtype.storm.task.IMetricsContext;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import java.util.Map;

/**
 * @author liujinxin
 * @since 2015-06-07 14:35
 */
public class OutbreakTrendFactory implements StateFactory {
    @Override
    public State makeState(Map map, IMetricsContext iMetricsContext, int i, int i1)
    {
        return new OutbreakTrendState(new OutbreakTrendBackingMap());
    }


}
