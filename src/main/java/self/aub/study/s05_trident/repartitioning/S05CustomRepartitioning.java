package self.aub.study.s05_trident.repartitioning;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;

import java.util.ArrayList;
import java.util.List;

/**
 * @author liujinxin
 * @since 2015-07-16 18:58
 */
public class S05CustomRepartitioning implements CustomStreamGrouping {
    private List<Integer> taskList;
    private int numCounterTasks;

    @Override
    public void prepare(WorkerTopologyContext workerTopologyContext, GlobalStreamId globalStreamId, List<Integer> list) {
        this.taskList = list;
        this.numCounterTasks = list.size();
    }

    @Override
    public List<Integer> chooseTasks(int i, List<Object> list) {
        List<Integer> boltIds = new ArrayList<>();
        if (list.size() > 0) {
            String str = list.get(0).toString();
            int index = 0;
            if (!str.isEmpty()) {
                index = str.charAt(0) % numCounterTasks;
            }
            boltIds.add(taskList.get(index));
        }
        return boltIds;
    }
}
