package countword.group;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by neal.xu on 2015/6/11.
 */
public class NumberGrouping implements CustomStreamGrouping {
    private List<Integer> targetTasks;

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        this.targetTasks = targetTasks;
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        List<Integer> boltIds = new ArrayList<>();
        if (values.size() > 0) {
            String str = values.get(0).toString();
            if (str.isEmpty()) {
                boltIds.add(targetTasks.get(0));
            } else {
                int i = str.charAt(0) % targetTasks.size();
                boltIds.add(targetTasks.get(i));
            }
        }
        return boltIds;
    }
}
