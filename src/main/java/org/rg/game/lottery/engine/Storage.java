package org.rg.game.lottery.engine;

import java.time.LocalDate;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public interface Storage extends AutoCloseable {
	static final String END_LINE_PREFIX = "Mr. Random suggerisce";

	public static String computeName(
		LocalDate extractionDate,
		int combinationCount,
		int numberOfCombos,
		String suffix
	) {
		return "[" + extractionDate.toString() + "]"+"[" + combinationCount +"]" +
			"[" + numberOfCombos + "]" + /*"[" + toRawString(numbers) + "]" +*/ suffix + ".txt";
	}

	int size();

	public String getName();

	boolean addCombo(List<Integer> selectedCombo);

	List<Integer> getCombo(int idx);

	boolean addLine(String value);

	void addUnindexedCombo(List<Integer> selectedCombo);

	void printAll();

	boolean addLine();

	@Override
	default void close() {

	}

	boolean isClosed();

	public Iterator<List<Integer>> iterator();

	Integer getMinOccurence();

	Integer getMaxOccurence();

	void delete();

	public  Map<Number, Integer> getHistoricalPremiums();

}
