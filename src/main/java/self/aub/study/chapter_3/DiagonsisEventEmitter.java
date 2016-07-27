package self.aub.study.chapter_3;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.ITridentSpout.Emitter;
import storm.trident.topology.TransactionAttempt;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author liujinxin
 * @since 2015-06-07 12:50
 */
public class DiagonsisEventEmitter implements Emitter<Long>, Serializable {
    private static final long serialVersionUID = 1l;
    private AtomicInteger successfulTransactions = new AtomicInteger(0);

    @Override
    public void emitBatch(TransactionAttempt transactionAttempt, Long aLong, TridentCollector tridentCollector) {
        for (int i = 0; i < 10000; i++) {
            List<Object> events = new ArrayList<>();
            double lat = new Double(-30 + (int) (Math.random() * 75));
            double lng = new Double(-120 + (int) (Math.random() * 70));
            long time = System.currentTimeMillis();
            String diag = new Integer(320 + (int) (Math.random() * 7)).toString();
            DiagnosisEvent event = new DiagnosisEvent(lat, lng, time, diag);
            events.add(event);
            tridentCollector.emit(events);
        }
    }

    @Override
    public void success(TransactionAttempt transactionAttempt) {
        successfulTransactions.incrementAndGet();
    }

    @Override
    public void close() {

    }
}
