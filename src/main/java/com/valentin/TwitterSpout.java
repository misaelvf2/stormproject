package com.valentin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import twitter4j.*;
import twitter4j.StatusListener;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterSpout extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(TwitterSpout.class);

    SpoutOutputCollector collector;

    String[] trackArray = { "$MSFT", "$NET" , "$PLTR", "$TSLA", "$AAPL", "$ETSY", "$AMZN", "$GME" };

    BlockingQueue<String> tweetQueue = new LinkedBlockingDeque<>(1000);

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;

        ConfigurationBuilder cb = new ConfigurationBuilder();

        cb.setDebugEnabled(true)
        .setOAuthConsumerKey("")
        .setOAuthConsumerSecret("")
        .setOAuthAccessToken("")
        .setOAuthAccessTokenSecret("");

        StatusListener listener = new StatusListener() {
            public void onStatus(Status status) {
                System.out.println(status.getUser().getName() + " : " + status.getText());
                tweetQueue.offer(status.getText());
            }

            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
            }

            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
            }

            public void onException(Exception ex) {
                ex.printStackTrace();
            }

            public void onScrubGeo(long x, long y) {
            }

            public void onStallWarning(StallWarning warning) {
            }
        };

        TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();

        twitterStream.addListener(listener);

        twitterStream.filter(new FilterQuery(trackArray));
    }

    @Override
    public void nextTuple() {
        try {
            String tweet = tweetQueue.poll(1000, TimeUnit.MILLISECONDS);
            String ticker;

            ticker = "";
            if (tweet != null && tweet.length() > 0) {
                for (String track : trackArray) {
                    if (tweet.contains(track)) {
                        ticker = track;
                        break;
                    }
                }
            }
            if (!ticker.equals("")) {
                collector.emit(new Values(ticker, tweet));
            }
            else {
                return;
            }
        } catch (InterruptedException e) {
            LOG.debug("No tweets yet...");
            return;
        }
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ticker", "tweet"));
    }

}
