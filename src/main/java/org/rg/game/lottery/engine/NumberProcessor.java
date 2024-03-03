package org.rg.game.lottery.engine;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.rg.game.core.TimeUtils;

public class NumberProcessor {

	public static final String EXCLUSIVE_KEY = "excl";
	public static final String INCLUSIVE_KEY = "in";
	public static final String PREVIOUS_KEY = "prev";
	public static final String RANDOM_KEY = "rand";
	public static final String MOST_EXTRACTED_KEY = "mostExt";
	public static final String MOST_EXTRACTED_COUPLE_KEY = "mostExtCouple";
	public static final String MOST_EXTRACTED_TRIPLE_KEY = "mostExtTriple";
	public static final String LESS_EXTRACTED_KEY = "lessExt";
	public static final String LESS_EXTRACTED_COUPLE_KEY = "lessExtCouple";
	public static final String LESS_EXTRACTED_TRIPLE_KEY = "lessExtTriple";
	public static final String NEAREST_FROM_RECORD_ABSENCE_PERCENTAGE_KEY = "nearFromAbsRecPerc";
	public static final String BIGGEST_ABSENCE_RECORD_KEY = "biggestAbsRec";
	public static final String SMALLEST_ABSENCE_RECORD_KEY = "smallestAbsRec";
	public static final String PREVIOUS_SYSTEM_KEY = PREVIOUS_KEY + "Sys";
	public static final String SKIP_KEY = "skip";
	public static final String NUMERICAL_SET_REG_EX =
		RANDOM_KEY + "|" + MOST_EXTRACTED_COUPLE_KEY + "|" + MOST_EXTRACTED_TRIPLE_KEY + "|" + MOST_EXTRACTED_KEY + "|" + LESS_EXTRACTED_COUPLE_KEY + "|" +
		LESS_EXTRACTED_TRIPLE_KEY + "|" + LESS_EXTRACTED_KEY + "|" + NEAREST_FROM_RECORD_ABSENCE_PERCENTAGE_KEY + "|" + BIGGEST_ABSENCE_RECORD_KEY + "|" + SMALLEST_ABSENCE_RECORD_KEY;

	public <E extends LotteryMatrixGeneratorAbstEngine> List<Integer> retrieveNumbersToBePlayed(
		Context<E> context,
		String numbersAsString,
		LocalDate extractionDate,
		boolean sorted
	) {
		return retrieveNumbers(
			context,
			numbersAsString, extractionDate, new ArrayList<>(),
			numberToBeIncluded -> numberToBeExcluded -> number ->
				(numberToBeIncluded.isEmpty() || numberToBeIncluded.contains(number)) && !numberToBeExcluded.contains(number),
			sorted
		);
	}

	public <E extends LotteryMatrixGeneratorAbstEngine> List<Integer> retrieveNumbersToBeExcluded(
		Context<E> context,
		String numbersAsString,
		LocalDate extractionDate,
		List<Integer> numbersToBePlayed,
		boolean sorted
	) {
		if (numbersAsString == null || numbersAsString.isEmpty()) {
			return new ArrayList<>();
		}
		List<Integer> numbersToBeExcluded = retrieveNumbers(
			context,
			numbersAsString, extractionDate, new ArrayList<>(),
			numberToBeIncluded -> numberToBeExcluded -> number ->
				(numberToBeIncluded.isEmpty() || numberToBeIncluded.contains(number)) && !numberToBeExcluded.contains(number) && numbersToBePlayed.contains(number),
			sorted
		);
		for(Integer number : numbersToBeExcluded) {
			numbersToBePlayed.remove(number);
		}
		return numbersToBeExcluded;
	}

	private <E extends LotteryMatrixGeneratorAbstEngine> List<Integer> retrieveNumbers(
		Context<E> context,
		String numbersAsString,
		LocalDate extractionDate,
		List<Integer> collectedNumbers,
		Function<List<Integer>, Function<List<Integer>, Predicate<Integer>>> numbersToBeIncludedPredicate,
		boolean sorted
	) {
		for (String numberAsString : numbersAsString.replaceAll("\\s+","").split(",")) {
			String[] rangeValues = numberAsString.split("->");
			if (rangeValues.length == 2) {
				String[] options = rangeValues[1].split(NUMERICAL_SET_REG_EX);
				Integer leftBound = Integer.parseInt(rangeValues[0]);
				Integer rightBound = null;
				if (options.length == 2) {
					Matcher numericalSetRegExMatcher = Pattern.compile("(" + NUMERICAL_SET_REG_EX + ")").matcher(rangeValues[1]);
					numericalSetRegExMatcher.find();
					String numberGeneratorType = numericalSetRegExMatcher.group(1);
					rangeValues[1] = options[0];
					rightBound = Integer.parseInt(rangeValues[1]);
					List<Integer> numberToBeIncluded = new ArrayList<>();
					Integer numbersToBeGenerated = null;
					if (options[1].contains(INCLUSIVE_KEY)) {
						String[] inclusionOptions = options[1].split(INCLUSIVE_KEY);
						numbersToBeGenerated = inclusionOptions[1].equalsIgnoreCase("all") ?
								(rightBound + 1) - leftBound :
								Integer.parseInt(inclusionOptions[0]);
						for (String numberToBeIncludedAsString : inclusionOptions[1].substring(0, inclusionOptions[1].indexOf(EXCLUSIVE_KEY) > -1?inclusionOptions[1].indexOf(EXCLUSIVE_KEY):inclusionOptions[1].length()).split("-")) {
							if (numberToBeIncludedAsString.contains(PREVIOUS_KEY)) {
								numberToBeIncluded.addAll(getPreviousNumbers(context,numberToBeIncludedAsString, collectedNumbers, extractionDate));
							} else {
								numberToBeIncluded.add(Integer.parseInt(numberToBeIncludedAsString));
							}
						}
					}
					List<Integer> numberToBeExcluded = new ArrayList<>();
					if (options[1].contains(EXCLUSIVE_KEY)) {
						String[] exclusionsOptions = options[1].split(EXCLUSIVE_KEY);
						if (numbersToBeGenerated == null) {
							numbersToBeGenerated = exclusionsOptions[0].equalsIgnoreCase("all") ?
									(rightBound + 1) - leftBound :
									Integer.parseInt(exclusionsOptions[0]);
						}
						for (String numberToBeExludedAsString : exclusionsOptions[1].split("-")) {
							if (numberToBeExludedAsString.contains(PREVIOUS_KEY)) {
								List<Integer> prevChosenNumbers = getPreviousNumbers(context, numberToBeExludedAsString, collectedNumbers, extractionDate);
								numberToBeExcluded.addAll(prevChosenNumbers);
								if (exclusionsOptions[0].equalsIgnoreCase("all")) {
									numbersToBeGenerated = numbersToBeGenerated - countNumbersInRange(prevChosenNumbers, leftBound, rightBound);
								}
							} else {
								numberToBeExcluded.add(Integer.parseInt(numberToBeExludedAsString));
								if (exclusionsOptions[0].equalsIgnoreCase("all")) {
									numbersToBeGenerated--;
								}
							}
						}
					}
					int skip = 0;
					if (options[1].contains(SKIP_KEY)) {
						String[] skipOptions = options[1].split(SKIP_KEY);
						skip = Integer.parseInt(skipOptions[1]);
						if (numbersToBeGenerated == null) {
							numbersToBeGenerated = skipOptions[0].equalsIgnoreCase("all") ?
									(rightBound + 1) - leftBound :
									Integer.parseInt(skipOptions[0]);
						}
					}
					List<Integer> generatedNumbers = new ArrayList<>();
					Iterator<Integer> numberGenerator = context.numberGeneratorFactory.apply(numberGeneratorType).apply(leftBound).apply(rightBound);
					if (skip > 0) {
						while (skip-- > 0) {
							numberGenerator.next();
						}
					}
					if (numbersToBeGenerated == null) {
						numbersToBeGenerated = options[1].equalsIgnoreCase("all") ?
							(rightBound + 1) - leftBound :
							Integer.parseInt(options[1]);
					}
					while (generatedNumbers.size() < numbersToBeGenerated) {
						Integer generatedNumber = numberGenerator.next();
						if (numbersToBeIncludedPredicate.apply(numberToBeIncluded).apply(numberToBeExcluded).test(generatedNumber) && !generatedNumbers.contains(generatedNumber)) {
							generatedNumbers.add(generatedNumber);
						}
					}
					collectedNumbers.addAll(generatedNumbers);
				} else {
					List<Integer> numberToBeIncluded = new ArrayList<>();
					if (options[0].contains(INCLUSIVE_KEY)) {
						String[] inclusionOptions = options[0].split(INCLUSIVE_KEY);
						rightBound = Integer.parseInt(rangeValues[1] = inclusionOptions[0]);
						for (String numberToBeIncludedAsString : inclusionOptions[1].substring(0, inclusionOptions[1].indexOf(EXCLUSIVE_KEY) > -1?inclusionOptions[1].indexOf(EXCLUSIVE_KEY):inclusionOptions[1].length()).split("-")) {
							if (numberToBeIncludedAsString.contains(PREVIOUS_KEY)) {
								numberToBeIncluded.addAll(getPreviousNumbers(context, numberToBeIncludedAsString, collectedNumbers, extractionDate));
							} else {
								numberToBeIncluded.add(Integer.parseInt(numberToBeIncludedAsString));
							}
						}
					}
					List<Integer> numberToBeExcluded = new ArrayList<>();
					if (options[0].contains(EXCLUSIVE_KEY)) {
						String[] exclusionsOptions = options[0].split(EXCLUSIVE_KEY);
						rightBound = Integer.parseInt(rangeValues[1] = exclusionsOptions[0]);
						for (String numberToBeExludedAsString : exclusionsOptions[1].split("-")) {
							if (numberToBeExludedAsString.contains(PREVIOUS_KEY)) {
								List<Integer> prevChosenNumbers = getPreviousNumbers(context, numberToBeExludedAsString, collectedNumbers, extractionDate);
								numberToBeExcluded.addAll(prevChosenNumbers);
							} else {
								numberToBeExcluded.add(Integer.parseInt(numberToBeExludedAsString));
							}
						}
					}
					int skip = 0;
					if (options[0].contains(SKIP_KEY)) {
						String[] skipOptions = options[0].split(SKIP_KEY);
						skip = Integer.parseInt(skipOptions[1]);
					}
					if (rightBound == null) {
						rightBound = Integer.parseInt(rangeValues[1]);
					}
					List<Integer> sequence = new ArrayList<>();
					for (int i = leftBound; i <= rightBound; i++) {
						if (skip > 0) {
							skip--;
							continue;
						}
						if (numbersToBeIncludedPredicate.apply(numberToBeIncluded).apply(numberToBeExcluded).test(i)) {
							sequence.add(i);
						}
					}
					collectedNumbers.addAll(sequence);
				}
			} else {
				if (rangeValues[0].contains(PREVIOUS_SYSTEM_KEY)) {
					List<Integer> numberToBeExcluded = new ArrayList<>();
					if (rangeValues[0].contains(EXCLUSIVE_KEY)) {
						String[] exclusionsOptions = rangeValues[0].split(EXCLUSIVE_KEY);
						rangeValues[0] = exclusionsOptions[0];
						for (String numberToBeExludedAsString : exclusionsOptions[1].split("-")) {
							if (numberToBeExludedAsString.contains(PREVIOUS_KEY)) {
								List<Integer> prevChosenNumbers = getPreviousNumbers(context, numberToBeExludedAsString, collectedNumbers, extractionDate);
								numberToBeExcluded.addAll(prevChosenNumbers);
							} else {
								numberToBeExcluded.add(Integer.parseInt(numberToBeExludedAsString));
							}
						}
					}
					int skip = 0;
					if (rangeValues[0].contains(SKIP_KEY)) {
						String[] skipOptions = rangeValues[0].split(SKIP_KEY);
						skip = Integer.parseInt(skipOptions[1]);
					}
					for (Integer number : getPreviousNumbers(context, rangeValues[0], collectedNumbers, extractionDate)) {
						if (skip > 0) {
							skip--;
							continue;
						}
						if (numbersToBeIncludedPredicate.apply(new ArrayList<>()).apply(numberToBeExcluded).test(number)) {
							collectedNumbers.add(number);
						}
					}
				} else {
					collectedNumbers.add(Integer.valueOf(rangeValues[0]));
				}
			}
		}
		if (sorted) {
			Collections.sort(collectedNumbers);
		}
		return collectedNumbers;
	}

	private Integer countNumbersInRange(List<Integer> prevChosenNumbers, Integer leftBound, Integer rightBound) {
		int count = 0;
		for (Integer number : prevChosenNumbers) {
			if (number >= leftBound && number <= rightBound) {
				count++;
			}
		}
		return count;
	}

	private <E extends LotteryMatrixGeneratorAbstEngine> List<Integer> getPreviousNumbers(Context<E> context, String exclusionsOptions, List<Integer> collector, LocalDate extractionDate) {
		return exclusionsOptions.startsWith(PREVIOUS_SYSTEM_KEY) ?
				exclusionsOptions.contains("discard") ?
					getNumbersFromPreviousSystems(context, exclusionsOptions.split("\\/"), extractionDate, "numbersToBeDiscarded") :
					getNumbersFromPreviousSystems(context, exclusionsOptions.split("\\/"), extractionDate, "chosenNumbers") :
			new ArrayList<>(collector);
	}

	private <E extends LotteryMatrixGeneratorAbstEngine> List<Integer> getNumbersFromPreviousSystems(
		Context<E> context,
		String[] numbersAsString,
		LocalDate extractionDate,
		String numbersType
	) {
		if (numbersAsString.length == 1) {
			return context.getNumbers(context.elaborationIndex - 1, extractionDate, numbersType);
		}
		List<Integer> chosenNumbers = new ArrayList<>();
		for (int i = 1; i < numbersAsString.length; i++) {
			chosenNumbers.addAll(
				context.getNumbers(context.elaborationIndex - Integer.valueOf(numbersAsString[i]), extractionDate, numbersType)
			);
		}
		return chosenNumbers;
	}

	public static String groupedForTenAsString(List<Integer> numbers, String numberSeparator, String tenSeparator) {
		return String.join(
			tenSeparator,
			groupedForTen(numbers).stream()
			.map(grp -> String.join(numberSeparator, grp.stream().map(Object::toString).collect(Collectors.toList())))
			.collect(Collectors.toList())
		);
	}

	public static List<List<Integer>> groupedForTen(List<Integer> numbers) {
		List<List<Integer>> grouped = new ArrayList<>();
		numbers = new ArrayList<>(numbers);
		Collections.sort(numbers);
		int min = 0;
		int max = 9;
		List<Integer> groupedForTen = null;
		for (Integer number : numbers) {
			while (!(number >= min && number <= max)) {
				min += 10;
				max += 10;
				if (groupedForTen == null || !groupedForTen.isEmpty()) {
					groupedForTen = new ArrayList<>();
					grouped.add(groupedForTen);
				}
			}
			if (groupedForTen == null) {
				groupedForTen = new ArrayList<>();
				grouped.add(groupedForTen);
			}
			groupedForTen.add(number);
		}
		return grouped;
	}

	public static String toSimpleString(List<Integer> numbers) {
		return String.join(
			",",
			numbers.stream()
		    .map(Object::toString)
		    .collect(Collectors.toList())
		);
	}

	public static String toRawString(List<Integer> numbers) {
		return String.join(
			"",
			numbers.stream()
		    .map(Object::toString)
		    .collect(Collectors.toList())
		);
	}

	static class Context<E extends LotteryMatrixGeneratorAbstEngine> {
		final Integer elaborationIndex;
		final List<Map.Entry<Supplier<E>, Supplier<Properties>>> allPreviousEngineAndConfigurations;
		final Function<String, Function<Integer, Function<Integer, Iterator<Integer>>>> numberGeneratorFactory;

		public Context(
			Function<String, Function<Integer, Function<Integer, Iterator<Integer>>>> numberGeneratorFactory,
			Integer elaborationIndex,
			List<Map.Entry<Supplier<E>, Supplier<Properties>>> allPreviousEngineAndConfigurations
		) {
			this.numberGeneratorFactory = numberGeneratorFactory;
			this.elaborationIndex = elaborationIndex;
			this.allPreviousEngineAndConfigurations = allPreviousEngineAndConfigurations;
		}

		public List<Integer> getNumbers(int index, LocalDate extractionDate, String numbersCollectionName) {
			E engine = allPreviousEngineAndConfigurations.get(index).getKey().get();
			Properties configuration = allPreviousEngineAndConfigurations.get(index).getValue().get();
			configuration.setProperty("competition", TimeUtils.defaultLocalDateFormat.format(extractionDate));
			return (List<Integer>)engine.setup(configuration, false).basicDataSupplier.apply(extractionDate).get(numbersCollectionName);
		}

	}

}
