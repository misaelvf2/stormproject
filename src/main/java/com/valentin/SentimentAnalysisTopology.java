package com.valentin;

import org.apache.storm.topology.ConfigurableTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class SentimentAnalysisTopology extends ConfigurableTopology {
    public static void main(String[] args) throws Exception {
        ConfigurableTopology.start(new SentimentAnalysisTopology(), args);
    }

    protected int run(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("tweets", new TwitterSpout());
        builder.setBolt("sentiment", new SentimentAnalysisBolt()).shuffleGrouping("tweets");
        // builder.setBolt("averageSentiment", new SentimentAveragerBolt()).fieldsGrouping("sentiment", new Fields("ticker"));

        conf.setDebug(false);

        String topologyName = "sentiment analysis";

        return submit(topologyName, conf, builder);
    }

}
