package br.com.stream.kafka.filters;

import org.apache.kafka.streams.kstream.Predicate;

public class RomeroFilter implements Predicate<Object, String> {
	
	private static final String FLAG_ROMERO = "romero";

	@Override
	public boolean test(Object key, String value) {
		System.out.println("filtro ROMERO");
		if (value.toLowerCase().contains(FLAG_ROMERO))
			return true;
		return false;
	}

}
