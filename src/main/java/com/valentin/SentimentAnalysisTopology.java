package com.valentin;

import org.apache.storm.topology.ConfigurableTopology;
import org.apache.storm.topology.TopologyBuilder;

public class SentimentAnalysisTopology extends ConfigurableTopology {
    public static void main(String[] args) throws Exception {
        ConfigurableTopology.start(new SentimentAnalysisTopology(), args);
    }

    protected int run(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("tweets", new TwitterSpout());
        builder.setBolt("sentiment", new SentimentAnalysisBolt()).shuffleGrouping("tweets");

        conf.setDebug(true);

        String topologyName = "sentiment analysis";

        return submit(topologyName, conf, builder);
    }

}
