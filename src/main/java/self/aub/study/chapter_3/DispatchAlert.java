package self.aub.study.chapter_3;



import com.esotericsoftware.minlog.Log;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * @author liujinxin
 * @since 2015-06-07 14:08
 */
public class DispatchAlert extends BaseFunction {
    private static final long serialVersionUID = 1l;
    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        String alert =(String) tridentTuple.getValue(0);
        Log.error("ALERT RECEIVED[" + alert + "]");
        Log.error("Dispatch the national guard!");
        System.exit(0);
    }
}
