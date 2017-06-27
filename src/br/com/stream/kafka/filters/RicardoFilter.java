package br.com.stream.kafka.filters;

import org.apache.kafka.streams.kstream.Predicate;

public class RicardoFilter implements Predicate<Object, String> {

	private static final String FLAG_RICARDO = "ricardo";
	
	@Override
	public boolean test(Object key, String value) {
		System.out.println("FILTRO RICARDO");
		if (value.toLowerCase().contains(FLAG_RICARDO))
			return true;
		return false;
	}
	

}
