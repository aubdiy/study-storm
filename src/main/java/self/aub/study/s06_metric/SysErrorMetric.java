package self.aub.study.s06_metric;

import backtype.storm.metric.api.IMetric;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author liujinxin
 * @since 2015-08-28 10:53
 */
public class SysErrorMetric implements IMetric {
    /**
     * 存储错误消息，key: error info, value: 出现次数
     */
    private Map<String, Integer> errorMsgMap = new HashMap<>();

    public void collect(String errorInfo) {
        Integer errorTimes = errorMsgMap.get(errorInfo);
        if (errorTimes == null) {
            errorTimes = 0;
        }
        ++errorTimes;
        errorMsgMap.put(errorInfo, errorTimes);
    }

    public void collect(Exception exception) {
        String message = exception.getMessage();
        Integer errorTimes = errorMsgMap.get(message);
        if (errorTimes == null) {
            errorTimes = 0;
        }
        ++errorTimes;
        errorMsgMap.put(message, errorTimes);
    }

    @Override
    public Object getValueAndReset() {
        Set<String> result = new LinkedHashSet<>();
        for (Map.Entry<String, Integer> errorMsg : errorMsgMap.entrySet()) {
            errorMsg.getKey();
            if (errorMsg.getValue() > 3) {
                result.add(errorMsg.getKey());
            }
        }
        errorMsgMap.clear();
        return result;
    }
}
