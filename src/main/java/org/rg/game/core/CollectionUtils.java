package org.rg.game.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class CollectionUtils {
	public static final CollectionUtils INSTANCE = new CollectionUtils();

	public <T> List<List<T>> toSubLists(List<T> inputList, int size) {
        final AtomicInteger counter = new AtomicInteger(0);
        List<List<T>> subLists = new ArrayList<>(inputList.stream()
                    .collect(Collectors.groupingBy((s -> counter.getAndIncrement()/size), LinkedHashMap::new, Collectors.toList()))
                    .values());
        return subLists;
	}

	public <A> int[] copyOf(int[] array) {
		int[] arrayCopy = new int[array.length];
		System.arraycopy(array, 0, arrayCopy, 0, array.length);
		return arrayCopy;
	}

	public Boolean retrieveBoolean(Properties config, String key) {
		return retrieveBoolean(config, key, null);
	}

	public Boolean retrieveBoolean(Properties config, String key, Boolean defaultValue) {
		String value = retrieveValue(config, key, defaultValue != null ? defaultValue.toString() : null);
		return value != null ?
			Boolean.parseBoolean(
				value.toLowerCase().replaceAll("\\s+","")
			) : null;
	}

	public Integer retrieveInteger(Properties config, String key) {
		return retrieveInteger(config, key, null);
	}

	public Integer retrieveInteger(Properties config, String key, Integer defaultValue) {
		String value = retrieveValue(config, key, defaultValue != null ? defaultValue.toString() : null);
		return value != null ?
			Integer.parseInt(
				value.toLowerCase().replaceAll("\\s+","")
			) : null;
	}

	public Long retrieveLong(Properties config, String key) {
		return retrieveLong(config, key, null);
	}

	public Long retrieveLong(Properties config, String key, Long defaultValue) {
		String value = retrieveValue(config, key, defaultValue != null ? defaultValue.toString() : null);
		return value != null ?
			Long.parseLong(
				value.toLowerCase().replaceAll("\\s+","")
			) : null;
	}

	public String retrieveValue(String key, String defaultValue) {
		String altKey = key.toUpperCase().replace(".", "_").replace("-", "_");
		return System.getProperty(
			key,
			System.getProperty(//-D command line parameters
				altKey,
				System.getenv().getOrDefault(//-D command line parameters
					key,
					System.getenv().getOrDefault(
						altKey,
						defaultValue
					)
				)
			)
		);
	}

	public String retrieveValue(Properties config, String key, String defaultValue) {
		String altKey = key.toUpperCase().replace(".", "_").replace("-", "_");
		return config != null ?
			config.getProperty(
				key,
				config.getProperty(
					altKey,
					retrieveValue(key, defaultValue)
				)
			) :
			retrieveValue(key, defaultValue);
	}

	public <T> T getLastElement(final Iterable<T> elements) {
	    T lastElement = null;
	    for (T element : elements) {
	        lastElement = element;
	    }
	    return lastElement;
	}
	public <T> Collection<T> even(Collection<T> elements) {
		Collection<T> items = new ArrayList<>();
		Iterator<T> elementsIterator = elements.iterator();
		int j = 0;
		while (elementsIterator.hasNext()) {
			T item = elementsIterator.next();
			if (j++ % 2 != 0) {
		    	items.add(item);
		    }
		}
		return items;
	}

	public <T> Collection<T> odd(Collection<T> elements) {
		Collection<T> items = new ArrayList<>();
		Iterator<T> elementsIterator = elements.iterator();
		int j = 0;
		while (elementsIterator.hasNext()) {
			T item = elementsIterator.next();
			if (j++ % 2 == 0) {
		    	items.add(item);
		    }
		}
		return items;
	}

}
