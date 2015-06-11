package countword.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

/**
 * Created by neal.xu on 2015/6/11.
 */
public class WordReaderSpout implements IRichSpout {

    private SpoutOutputCollector collector;
    private TopologyContext context;
    private FileReader fileReader;
    private boolean completed = false;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        try {
            this.fileReader = new FileReader(conf.get("wordFile").toString());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        this.context = context;
        this.collector = collector;
        System.out.println("Thread start with Spout :" + Thread.currentThread().getName());
    }

    @Override
    public void close() {
        System.out.println("Close!");

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void nextTuple() {
        String line;
        BufferedReader reader = new BufferedReader(fileReader);
        try {
            while ((line = reader.readLine()) != null) {
                // message id test
                this.collector.emit(new Values(line));
            }
        } catch (IOException e) {
            throw new RuntimeException("Error reading tuple", e);
        } finally {
            this.completed = true;
        }
    }

    @Override
    public void ack(Object msgId) {
        System.out.println("Success : " + msgId);
    }

    @Override
    public void fail(Object msgId) {
        System.out.println("Fail : " + msgId);
    }
}
