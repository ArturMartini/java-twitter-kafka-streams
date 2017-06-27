package br.com.stream;

import br.com.stream.kafka.KafkaStreamRedirect;
import br.com.stream.twitter.StreamTwitter;

public class CieloKafkaExample {
	
	public static void main(String[] args) {

		StreamTwitter twitterStream = new StreamTwitter(); 
		twitterStream.start();
		
		KafkaStreamRedirect kafkaStream = new KafkaStreamRedirect();
		kafkaStream.startStream();
		
	}

}
