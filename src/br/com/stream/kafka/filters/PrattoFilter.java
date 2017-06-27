package br.com.stream.kafka.filters;

import org.apache.kafka.streams.kstream.Predicate;

public class PrattoFilter implements Predicate<Object, String> {
	
	private static final String FLAG_PRATTO = "pratto";

	@Override
	public boolean test(Object key, String value) {
		System.out.println("filtro PRATTO");
		if (value.toLowerCase().contains(FLAG_PRATTO))
			return true;
		return false;
	}
	
	

}
