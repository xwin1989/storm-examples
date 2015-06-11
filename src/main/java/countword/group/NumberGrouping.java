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
    private int numTasks = 0;

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        this.numTasks = targetTasks.size();
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        List<Integer> boltIds = new ArrayList<>();
        if (!values.isEmpty()) {
            String str = values.get(0).toString();
            if (str.isEmpty()) {
                boltIds.add(0);
            } else {
                boltIds.add(Integer.parseInt(str) % numTasks);
            }
        }
        return boltIds;
    }
}
