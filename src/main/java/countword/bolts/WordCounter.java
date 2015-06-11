package countword.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by neal.xu on 2015/6/11.
 */
public class WordCounter implements IRichBolt {
    private Integer id;
    private String name;
    private Map<String, Integer> counters;
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.counters = new HashMap<>();
        this.collector = collector;
        this.name = context.getThisComponentId();
        this.id = context.getThisTaskId();
        System.out.println("Thread start with name :" + Thread.currentThread().getName());
    }

    @Override
    public void execute(Tuple input) {
        String str = null;
        try {
            str = input.getStringByField("word");
        } catch (IllegalArgumentException e) {
            //Do nothing
        }

        if (str != null) {
            if (!counters.containsKey(str)) {
                counters.put(str, 1);
            } else {
                Integer c = counters.get(str) + 1;
                counters.put(str, c);
            }
        } else {
            if ("signals".equals(input.getSourceStreamId())) {
                str = input.getStringByField("action");
                if ("refreshCache".equals(str))
                    counters.clear();
            }
        }
        //Set the tuple as Acknowledge
        collector.ack(input);
    }

    @Override
    public void cleanup() {
        System.out.println("-- count [" + name + "/" + id + "] --");
        for (Map.Entry<String, Integer> entry : counters.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
