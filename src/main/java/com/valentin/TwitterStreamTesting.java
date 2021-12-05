package com.valentin;

import java.io.IOException;
import java.util.ArrayList;

import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterStreamTesting {
    public static void main(String[] args) throws TwitterException, IOException {
        ConfigurationBuilder cb = new ConfigurationBuilder();

        cb.setDebugEnabled(true)
                .setOAuthConsumerKey("Zs6WUdAbp3N5FjOrgSvjj84vG")
                .setOAuthConsumerSecret("3T7GjyYcIu5BjdIhxnZrMaAwQgyq4tAEwWQN6cB6PncWba9aP5")
                .setOAuthAccessToken("438836583-BwICiH63T5OVYP11KRj3PTsJ8dRL3yo8BJxLxnZY")
                .setOAuthAccessTokenSecret("hyGxgqrToTbB28dVbjy9zgO0yjlQNyQ6eMDhHfm2yBasp");

        StatusListener listener = new StatusListener() {
            public void onStatus(Status status) {
                System.out.println(status.getUser().getName() + " : " + status.getText());
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

        ArrayList<String> track = new ArrayList<String>();

        for (String arg : args) {
            track.add(arg);
        }

        String[] trackArray = track.toArray(new String[track.size()]);

        twitterStream.filter(new FilterQuery(trackArray));
    }
}
