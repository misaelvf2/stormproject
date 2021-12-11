package com.valentin;

import java.util.HashMap;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Properties;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations.SentimentAnnotatedTree;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

public class SentimentAnalysisBolt extends BaseRichBolt {
    static StanfordCoreNLP pipeline;

    Map<String, String> sentimentMap;

    OutputCollector collector;

    public static void init() {
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
        pipeline = new StanfordCoreNLP(props);
    }

    public static String findSentiment(String tweet) {
        String sentimentType = "NULL";
        int sentiment = 0;

        if (tweet != null && tweet.length() > 0) {
            Annotation annotation = pipeline.process(tweet);
            for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
                Tree tree = sentence.get(SentimentAnnotatedTree.class);
                sentiment = RNNCoreAnnotations.getPredictedClass(tree);
                sentimentType = sentence.get(SentimentCoreAnnotations.SentimentClass.class);
            }
        }
        return sentimentType;
    }

    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.sentimentMap = new HashMap<String, String>();
        init();
    }

    @Override
    public void execute(Tuple tuple) {
        String ticker = tuple.getString(0);
        String tweet = tuple.getString(1);
        String sentiment = findSentiment(tweet);

        // if (!sentimentMap.containsKey(tweet)) {
        //     sentimentMap.put(tweet, sentiment);
        //  } else {
        //     sentimentMap.put(tweet, sentiment);
        //  }

        collector.emit(tuple, new Values(ticker, tweet, findSentiment(tweet)));
        collector.ack(tuple);
    }

    // @Override
    // public void cleanup() {
    //     // for(Map.Entry<String, String> entry : sentimentMap.entrySet()){
    //     //     System.out.println(entry.getKey() + " : " + entry.getValue());
    //     //  }
    // }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ticker", "tweet", "sentiment"));
    }

}
