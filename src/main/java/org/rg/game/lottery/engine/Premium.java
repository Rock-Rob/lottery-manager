package org.rg.game.lottery.engine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class Premium {
	public static final Integer TYPE_TWO = 2;
	public static final Integer TYPE_THREE = 3;
	public static final Integer TYPE_FOUR = 4;
	public static final Integer TYPE_FIVE = 5;
	public static final Double TYPE_FIVE_PLUS = 5.5d;
	public static final Integer TYPE_SIX = 6;


	public static final String LABEL_TWO = "Ambo";
	public static final String LABEL_THREE = "Terno";
	public static final String LABEL_FOUR = "Quaterna";
	public static final String LABEL_FIVE = "Cinquina";
	public static final String LABEL_FIVE_PLUS = "Cinquina + Jolly";
	public static final String LABEL_SIX = "Tombola";

	private static final Map<Number, String> all;
	private static final List<String> allLabelsList;
	private static final List<Number> allTypesList;
	private static final List<Number> allTypesListReversed;
	private static final Number[] allTypes;
	private static final Number[] allTypesReversed;
	private static final List<Number> allHighTypesList;
	private static final Number[] allHighTypes;

	static {
		all = new LinkedHashMap<>();
		all.put(TYPE_TWO, LABEL_TWO);
		all.put(TYPE_THREE, LABEL_THREE);
		all.put(TYPE_FOUR, LABEL_FOUR);
		all.put(TYPE_FIVE, LABEL_FIVE);
		all.put(TYPE_FIVE_PLUS, LABEL_FIVE_PLUS);
		all.put(TYPE_SIX, LABEL_SIX);
		allLabelsList = new ArrayList<>(all.values());
		allTypesList = new ArrayList<>(all.keySet());
		allTypesListReversed = new ArrayList<>(allTypesList);
		Collections.reverse(allTypesListReversed);
		allTypes = allTypesList.toArray(new Number[allTypesList.size()]);
		allTypesReversed = allTypesListReversed.toArray(new Number[allTypesListReversed.size()]);
		allHighTypesList = new ArrayList<>(
			Arrays.asList(
				Premium.toType(Premium.LABEL_FIVE),
				Premium.toType(Premium.LABEL_FIVE_PLUS),
				Premium.toType(Premium.LABEL_SIX)
			)
		);
		allHighTypes = allHighTypesList.toArray(new Number[allHighTypesList.size()]);
	}

	public static List<String> allLabelsList() {
		return allLabelsList;
	}

	public static Map<Number, String> all() {
		return all;
	}

	public static List<Number> allTypesList() {
		return allTypesList;
	}

	public static Number[] allTypes() {
		return allTypes;
	}

	public static Number[] allTypesReversed() {
		return allTypesReversed;
	}

	public static List<Number> allTypesListReversed() {
		return allTypesListReversed;
	}

	public static List<Number> allHighTypesList() {
		return allHighTypesList;
	}

	public static Number[] allHighTypes() {
		return allHighTypes;
	}

	public static String toLabel(Number hit) {
		String label = all.entrySet().stream()
			.filter(entry -> entry.getKey().doubleValue() == hit.doubleValue())
			.map(Map.Entry::getValue).findFirst().orElseGet(() -> null);
		if (label != null) {
			return label;
		}
		throw new IllegalArgumentException("Unvalid premium type: " + hit);
	}

	public static List<String> toLabels(List<Number> hits) {
		List<String> labels = new ArrayList<>();
		for (Map.Entry<Number, String> typeAndLabel : all.entrySet()) {
			for (Number hit : hits) {
				if (hit.doubleValue() == typeAndLabel.getKey().doubleValue()) {
					labels.add(typeAndLabel.getValue());
					break;
				}
			}
		}
		return labels;
	}

	public static Number parseType(String typeAsString) {
		Double type = Double.valueOf(typeAsString);
		if (type.compareTo(TYPE_FIVE_PLUS) == 0) {
			return TYPE_FIVE_PLUS;
		}
		return Integer.parseInt(typeAsString);
	}

	public static Number toType(String label) {
		for (Entry<Number, String> entry : all.entrySet()) {
			if (entry.getValue().equalsIgnoreCase(label)) {
				return entry.getKey();
			}
		}
		throw new IllegalArgumentException("Unvalid premium label: " + label);
	}

	public static String toString(Map<Number, Integer> premiums, String keyValueSeparator, String premiumSeparator) {
		StringBuffer buffer = new StringBuffer();
		Iterator<Map.Entry<Number, Integer>> entryIterator = premiums.entrySet().iterator();
		while (entryIterator.hasNext()) {
			Map.Entry<Number, Integer> entry = entryIterator.next();
			buffer.append(toLabel(entry.getKey()) + keyValueSeparator + entry.getValue());
			if (entryIterator.hasNext()) {
				buffer.append(premiumSeparator);
			}
		}
		return buffer.toString();
	}

}
