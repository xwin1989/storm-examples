package countword;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import countword.bolts.WordCounter;
import countword.bolts.WordStandard;
import countword.spouts.WordReaderSpout;

/**
 * Created by neal.xu on 2015/6/11.
 */
public class TopologyMain {
    public static void main(String[] args) throws InterruptedException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader", new WordReaderSpout());
        builder.setBolt("word-standard", new WordStandard()).shuffleGrouping("word-reader");
        builder.setBolt("word-counter", new WordCounter(), 2)
                .fieldsGrouping("word-standard", new Fields("word"));

        Config config = new Config();
        config.put("wordFile", "E:\\words.txt");
        config.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("count-word-topology-With-Refresh-Cache", config, builder.createTopology());
        Thread.sleep(5000);
        cluster.shutdown();
    }

    public String getPath(String source) {
        return getClass().getResource(source).getPath();
    }
}
