package br.com.stream.twitter;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

public class StreamTwitter {
	
	private static final Producer<String, String> producer;
	
	static{
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		producer = new KafkaProducer<>(props);
	}
	
	public void start() {
	    
		StatusListener listener = new StatusListener(){
	        public void onStatus(Status status) {
	        	String tweet = "@" + status.getUser().getScreenName() + ":" + status.getText();
		        System.out.println(tweet);
		        String key = "";
		        
		        if(tweet.toLowerCase().contains("pratto"))
		        	key = "pratto";
		        if(tweet.toLowerCase().contains("borja"))
		        	key = "borja";
		        if(tweet.toLowerCase().contains("ricardo"))
		        	key = "ricardo";
		        if(tweet.toLowerCase().contains("romero"))
		        	key = "romero";
		        
		        
		        producer.send(new ProducerRecord<String, String>("twitter-input", key, tweet));
	        }
	        public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
	        public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}
	        public void onException(Exception ex) {
	            ex.printStackTrace();
	        }
			public void onScrubGeo(long arg0, long arg1) {}
			public void onStallWarning(StallWarning arg0) {}
	    };
	    
	    
	    ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
		  .setOAuthConsumerKey("Mp8JmFogV01R3FXKXNNt9inc1")
		  .setOAuthConsumerSecret("NLR9naTtVIzfhWE9zwFpQKHoVc3StlATpX5R9Neg7oy0XIyYV4")
		  .setOAuthAccessToken("3021157127-YAylPbs6OGaz79cgUxqZUveJBM7dym1ItJLxKEb")
		  .setOAuthAccessTokenSecret("KFD9FqAuKANkEsnlAuWBYvTbKiRCGJUABSAb5m3fk8ewC");
	    
	    TwitterStreamFactory factory = new TwitterStreamFactory(cb.build());
	    TwitterStream twitterStream = factory.getInstance();
	    
	    FilterQuery fq = new FilterQuery();
	    String keywords[] = {"#ApacheKafkaArtur"};
	    fq.track(keywords);
	    
	    twitterStream.addListener(listener);
	    twitterStream.filter(fq);
	    
	}

}

