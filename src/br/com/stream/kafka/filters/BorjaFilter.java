package br.com.stream.kafka.filters;

import org.apache.kafka.streams.kstream.Predicate;

public class BorjaFilter implements Predicate<Object, String> {

	private static final String FLAG_BORJA = "borja";
	
	@Override
	public boolean test(Object key, String value) {
		System.out.println("filtro BORJA");
		if (value.toLowerCase().contains(FLAG_BORJA))
			return true;
		return false;
	}

}
