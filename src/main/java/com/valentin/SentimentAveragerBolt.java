package com.valentin;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
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

public class SentimentAveragerBolt extends BaseRichBolt {
    static StanfordCoreNLP pipeline;

    Map<String, Integer> sentimentIntegerMap;
    Map<Integer, String> integerToSentiment;
    Map<String, LinkedBlockingDeque<Integer>> tickerSentiments;

    OutputCollector collector;

    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.sentimentIntegerMap = new HashMap<String, Integer>();
        this.integerToSentiment = new HashMap<Integer, String>();
        this.tickerSentiments = new HashMap<String, LinkedBlockingDeque<Integer>>();

        integerToSentiment.put(0, "very negative");
        integerToSentiment.put(1, "negative");
        integerToSentiment.put(2, "neutral");
        integerToSentiment.put(3, "positive");
        integerToSentiment.put(4, "very positive");
    }

    @Override
    public void execute(Tuple tuple) {
        String ticker = tuple.getString(0);
        String tweet = tuple.getString(1);
        int sentiment = tuple.getInteger(2);

        if (!tickerSentiments.containsKey(ticker)) {
            tickerSentiments.put(ticker, new LinkedBlockingDeque<Integer>(5));
            tickerSentiments.get(ticker).add(sentiment);
        }
        else {
            tickerSentiments.get(ticker).add(sentiment);
        }

        // if (!sentimentMap.containsKey(tweet)) {
        //     sentimentMap.put(tweet, sentiment);
        //  } else {
        //     sentimentMap.put(tweet, sentiment);
        //  }

        collector.emit(tuple, new Values(tweet));
        collector.ack(tuple);
    }

    @Override
    public void cleanup() {
        // for(Map.Entry<String, String> entry : sentimentMap.entrySet()){
        //     System.out.println(entry.getKey() + " : " + entry.getValue());
        //  }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet", "sentiment"));
    }

}
