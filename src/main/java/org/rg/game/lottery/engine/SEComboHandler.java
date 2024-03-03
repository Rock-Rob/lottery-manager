package org.rg.game.lottery.engine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class SEComboHandler {

	public static String toWAString(
		Collection<Integer> combo,
		String separator,
		String jollySeparator,
		Collection<Integer> numbers
	) {
		Integer jolly = null;
		Collection<Integer> originalCombo = combo;
		Collection<Integer> originalNumbers = numbers;
		if (combo.size() > 6) {
			combo = new ArrayList<>(combo);
			jolly = ((List<Integer>)combo).get(6);
			combo = new ArrayList<>(combo).subList(0, 6);
		} else if (numbers.size() > 6) {
			numbers = new ArrayList<>(numbers);
			jolly = ((List<Integer>)numbers).get(6);
			numbers = new ArrayList<>(numbers).subList(0, 6);
		}
		Collection<Integer> finalNumbers = numbers;
		AtomicInteger hitCount = new AtomicInteger(0);
		String wAString = String.join(
			separator,
			combo.stream()
		    .map(val -> {
		    	boolean hit = finalNumbers.contains(val);
		    	if (hit) {
		    		hitCount.incrementAndGet();
		    	}
		    	return (hit ? "*" : "") + val.toString() + (hit ? "*" : "");
		    })
		    .collect(Collectors.toList())
		);
		if (jolly != null) {
			String jollyAsString = null;
			if (originalCombo.size() > 6) {
				if (originalNumbers.contains(jolly) && hitCount.get() == Premium.TYPE_FIVE) {
					jollyAsString = "_*" + jolly + "*_";
				} else {
					jollyAsString = jolly.toString();
				}
				wAString = String.join(
					jollySeparator,
					Arrays.asList(
						wAString,
						jollyAsString
					)
				);
			} else if (originalNumbers.size() > 6) {
				if (originalNumbers.contains(jolly) && hitCount.get() == Premium.TYPE_FIVE) {
					wAString = wAString.replace(jolly.toString(), "_*" + jolly + "*_");
				}
			}
		}
		return wAString;
	}

	public static String toString(List<Integer> combo, String separator, String jollySeparator) {
		Iterator<Integer> comboIterator = combo.iterator();
		String comboAsString = "";
		int index = 0;
		while (index < 6) {
			comboAsString = comboAsString + comboIterator.next();
			if (++index < 6) {
				comboAsString = comboAsString + separator;
			}
		}
		if (comboIterator.hasNext()) {
			comboAsString = comboAsString + jollySeparator + comboIterator.next();
		}
		return comboAsString;
	}

}
