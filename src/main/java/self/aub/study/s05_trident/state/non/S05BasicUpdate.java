package self.aub.study.s05_trident.state.non;

import backtype.storm.tuple.Values;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

import java.util.List;

/**
 * @author liujinxin
 * @since 2015-07-17 17:18
 */
public class S05BasicUpdate extends BaseStateUpdater<S05BasicState> {
//    private static final Logger LOG = LoggerFactory.getLogger(S05NoTransactionalUpdate.class);

    @Override
    public void updateState(S05BasicState s05BasicState, List<TridentTuple> list, TridentCollector tridentCollector) {
        for (TridentTuple tridentTuple : list) {
            String city = tridentTuple.getStringByField("city");
            String population = s05BasicState.getPopulation(city);
            tridentCollector.emit(new Values(city, population));
        }
    }
}
