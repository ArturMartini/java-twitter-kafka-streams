package br.com.stream.kafka;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import br.com.stream.kafka.filters.BorjaFilter;
import br.com.stream.kafka.filters.PrattoFilter;
import br.com.stream.kafka.filters.RicardoFilter;
import br.com.stream.kafka.filters.RomeroFilter;

public class KafkaStreamRedirect {
	
	private static final Map<String, Object> props = new HashMap<>();
	private static final StreamsConfig config;
	
	static {
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streams");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		config = new StreamsConfig(props);
	}
	
	public void startStream(){

		KStreamBuilder builder = new KStreamBuilder();
		
		KStream<Object, String> stream = builder.stream("twitter-input");	
		
		stream.filter(new PrattoFilter()).mapValues(value -> transformation(value)).to("twitter-pratto");
		stream.filter(new BorjaFilter()).mapValues(value -> transformation(value)).to("twitter-borja");
		stream.filter(new RicardoFilter()).mapValues(value -> transformation(value)).to("twitter-ricardo");
		stream.filter(new RomeroFilter()).mapValues(value -> transformation(value)).to("twitter-romero");
		
		KafkaStreams streams = new KafkaStreams(builder, config);
		
		try{
			streams.start();
		}catch(Exception ex){
			//tratamento de erro
		}
		
	}

	private String transformation(Object value) {
		// Processo de transformação JSON / XML
		return (String)value.toString().toLowerCase();
	}

}
