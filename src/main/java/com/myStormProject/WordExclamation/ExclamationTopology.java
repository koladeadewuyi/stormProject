package com.myStormProject.WordExclamation;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class ExclamationTopology {
    public static void main (String[] args) {
        Config conf = createConfig();
        StormTopology topology = buildTopology();
        LocalCluster cluster = runTopologyOnCluster(conf, topology);
        stopCluster(cluster);
    }

    private static Config createConfig() {
        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(2);
        return conf;
    }

    private static StormTopology buildTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("words", new TestWordSpout(), 2);
        builder.setBolt("exclaim1", new ExclamationBolt(), 3)
                .shuffleGrouping("words");
        builder.setBolt("exclaim2", new ExclamationBolt(), 2)
                .shuffleGrouping("exclaim1");
        return builder.createTopology();
    }

    private static LocalCluster runTopologyOnCluster(Config conf, StormTopology topology) {
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("exclamationTopology", conf, topology);
        Utils.sleep(10000);
        return cluster;
    }

    private static void stopCluster(LocalCluster cluster) {
        cluster.killTopology("exclamationTopology");
        cluster.shutdown();
    }
}
