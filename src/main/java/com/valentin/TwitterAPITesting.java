package com.valentin;

import java.io.IOException;

import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterAPITesting {

    public static void main(String[] args) throws TwitterException, IOException {
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
          .setOAuthConsumerKey("Zs6WUdAbp3N5FjOrgSvjj84vG")
          .setOAuthConsumerSecret("3T7GjyYcIu5BjdIhxnZrMaAwQgyq4tAEwWQN6cB6PncWba9aP5")
          .setOAuthAccessToken("438836583-BwICiH63T5OVYP11KRj3PTsJ8dRL3yo8BJxLxnZY")
          .setOAuthAccessTokenSecret("hyGxgqrToTbB28dVbjy9zgO0yjlQNyQ6eMDhHfm2yBasp");

        TwitterFactory tf = new TwitterFactory(cb.build());
        Twitter twitter = tf.getInstance();

        Query query = new Query("$MSFT");
        QueryResult result = twitter.search(query);

        for (Status status : result.getTweets()) {
            System.out.println("@" + status.getUser().getScreenName() + ":" + status.getText());
        }
    }
}