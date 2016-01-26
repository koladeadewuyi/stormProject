package com.myStormProject.WordCount;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class WordCountTopology {
    public static void main(String[] args) {
        Config conf = createConfig();
        StormTopology topology = createTopology();
        LocalCluster cluster = runTopologyOnCluster(conf, topology);
        stopCluster(cluster);
    }

    private static void stopCluster(LocalCluster cluster) {
        cluster.killTopology("wordCountTopology");
        cluster.shutdown();
    }

    private static LocalCluster runTopologyOnCluster(Config conf, StormTopology topology) {
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("wordCountTopology", conf, topology);
        Utils.sleep(10000);
        return cluster;
    }

    private static StormTopology createTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("sentences", new RandomSentencesSpout(), 1);
        builder.setBolt("split", new SplitSentence(), 8).shuffleGrouping("sentences");
        builder.setBolt("count", new WordCount(), 12).fieldsGrouping("split", new Fields("words"));
        return builder.createTopology();
    }

    private static Config createConfig() {
        Config conf = new Config();
        conf.setNumWorkers(2);
        conf.setDebug(true);
        return conf;
    }
}
