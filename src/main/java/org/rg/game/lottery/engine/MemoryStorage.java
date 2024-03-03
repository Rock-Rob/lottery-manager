package org.rg.game.lottery.engine;

import java.io.BufferedReader;
import java.io.StringReader;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.burningwave.Synchronizer;
import org.burningwave.Throwables;
import org.rg.game.core.LogUtils;
import org.rg.game.core.MathUtils;

public class MemoryStorage implements Storage {

	List<List<Integer>> combos = new ArrayList<>();
	String output;
	String name;
	boolean isClosed;
	Map<Integer, Integer> occurrences;
	Map<Number, Integer> historicalPremiums;

	MemoryStorage(
		LocalDate extractionDate,
		int combinationCount,
		int numberOfCombos,
		String group,
		String suffix
	) {
		name = "[" + extractionDate.toString() + "]"+"[" + combinationCount +"]" +
				"[" + numberOfCombos + "]" + /*"[" + toRawString(numbers) + "]" +*/ suffix + ".txt";
		combos = new ArrayList<>();
		output = "";
		occurrences = new LinkedHashMap<>();
	}

	@Override
	public int size() {
		return combos.size();
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public List<Integer> getCombo(int idx) {
		return combos.get(idx);
	}

	@Override
	public boolean addCombo(List<Integer> combo) {
		if (!contains(combo)) {
			output += ComboHandler.toString(combo) + "\n";
			for (Integer number : combo) {
				Integer counter = occurrences.computeIfAbsent(number, key -> 0) + 1;
				occurrences.put(number, counter);
			}
			return combos.add(combo);
		}
		return false;
	}

	public boolean contains(List<Integer> selectedCombo) {
		for (List<Integer> combo : combos) {
			for (Integer number : combo) {
				if (!selectedCombo.contains(number)) {
					return false;
				}
			}
		}
		return !combos.isEmpty();
	}

	@Override
	public void addUnindexedCombo(List<Integer> selectedCombo) {
		addLine(ComboHandler.toString(selectedCombo));
	}

	@Override
	public void printAll() {
		LogUtils.INSTANCE.info(output);
	}

	@Override
	public boolean addLine(String value) {
		output += "\n" + value;
		return true;
	}

	@Override
	public boolean addLine() {
		output += "\n";
		return true;
	}

	@Override
	public void delete() {
		combos.clear();
		output = "";
	}

	@Override
	public Iterator<List<Integer>> iterator() {
		return new Iterator<List<Integer>>() {
			int currentIndex = 0;
			@Override
			public List<Integer> next() {
				return combos.get(currentIndex++);
			}

			@Override
			public boolean hasNext() {
				try {
					combos.get(currentIndex);
					return true;
				} catch (IndexOutOfBoundsException exc) {
					return false;
				}
			}
		};
	}

	@Override
	public void close() {
		isClosed = true;
	}

	@Override
	public boolean isClosed() {
		return isClosed;
	}

	@Override
	public Integer getMinOccurence() {
		if (size() == 0) {
			return 0;
		}
		TreeSet<Integer> occurrencesCounter = new TreeSet<>(occurrences.values());
		return occurrencesCounter.iterator().next();
	}

	@Override
	public Integer getMaxOccurence() {
		if (size() == 0) {
			return 0;
		}
		TreeSet<Integer> occurrencesCounter = new TreeSet<>(occurrences.values());
		return occurrencesCounter.descendingIterator().next();
	}

	@Override
	public Map<Number, Integer> getHistoricalPremiums() {
		if (this.historicalPremiums == null) {
			Synchronizer.INSTANCE.execute(this + "_computeHistoricalPremiums", () -> {
				if (this.historicalPremiums == null) {
					Map<Number, Integer> historicalPremiums = new LinkedHashMap<>();
					try (BufferedReader br = new BufferedReader(new StringReader(output))) {
				        String line;
				        boolean startToCollect = false;
				        while ((line = br.readLine()) != null) {
				           if (line.contains("Riepilogo risultati storici sistema dal")) {
				        	   startToCollect = true;
				        	   continue;
				           } else if (line.contains("Costo:")) {
				        	  break;
				           }
				           if (startToCollect && !(line = line.replaceAll("\\s+","")).isEmpty()) {
				        	   String[] premiumLabelAndCounter = line.split(":");
				        	   historicalPremiums.put(
			        			   Premium.toType(premiumLabelAndCounter[0]),
			        			   MathUtils.INSTANCE.integerFormat.parse(premiumLabelAndCounter[1]).intValue()
		        			   );
				           }
				        }
				    } catch (Throwable e) {
						Throwables.INSTANCE.throwException(e);
					}
					this.historicalPremiums = historicalPremiums;
				}
			});
		}
		return historicalPremiums;
	}

}
