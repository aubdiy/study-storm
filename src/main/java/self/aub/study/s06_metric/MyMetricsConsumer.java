package self.aub.study.s06_metric;

import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * @author liujinxin
 * @since 2015-08-25 14:36
 */
public class MyMetricsConsumer implements IMetricsConsumer {


    public static final Logger LOG = LoggerFactory.getLogger(MyMetricsConsumer.class);
    private static String padding = "                       ";


    @Override
    public void prepare(Map stormConf, Object registrationArgument, TopologyContext context, IErrorReporter errorReporter) {
    }
    @Override
    public void handleDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
        StringBuilder sb = new StringBuilder();
        String header = String.format("%d\t%15s:%-4d\t%3d:%-11s\t",
                new Object[]{Long.valueOf(taskInfo.timestamp), taskInfo.srcWorkerHost, Integer.valueOf(taskInfo.srcWorkerPort), Integer.valueOf(taskInfo.srcTaskId), taskInfo.srcComponentId});
        sb.append(header);

        Iterator i$ = dataPoints.iterator();

        while(i$.hasNext()) {
            DataPoint p = (DataPoint)i$.next();
            sb.delete(header.length(), sb.length());
            sb.append(p.name).append(padding).delete(header.length() + 23, sb.length()).append("\t").append(p.value);


            LOG.info(sb.toString());
        }

    }
    @Override
    public void cleanup() {
    }


}
