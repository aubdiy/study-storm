package self.aub.study.s05_trident.state.non;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.state.State;

import java.util.HashMap;
import java.util.Map;

/**
 * @author liujinxin
 * @since 2015-07-17 16:56
 */
public class S05BasicState implements State {
    private static final Logger LOG = LoggerFactory.getLogger(S05BasicState.class);

    private Map<String, String> map = new HashMap<>();

    public S05BasicState() {
        map.put("北京", "2151万人");
        map.put("上海", "2500万人");
        map.put("广州", "1292万人");
        map.put("深圳", "1062万人");
        map.put("天津", "1472万人");
        map.put("杭州", "884万人");
        map.put("沈阳", "825万人");
        map.put("长春", "790万人");
        map.put("济南", "694万人");
        map.put("苏州", "1300万人");
    }

    @Override
    public void beginCommit(Long txid) {
        LOG.info("State beginCommit ==>> txid:{} obj:{}", txid, this.hashCode());

    }

    @Override
    public void commit(Long txid) {
        LOG.info("State commit ==>> txid:{}", txid);
    }

    public String getPopulation(String city) {
        return map.get(city);
    }
}
