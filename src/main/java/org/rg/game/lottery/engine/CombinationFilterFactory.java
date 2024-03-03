package org.rg.game.lottery.engine;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.DoublePredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.rg.game.core.LogUtils;

public class CombinationFilterFactory {
	public static final CombinationFilterFactory INSTANCE;
	private PredicateExpressionParser<List<Integer>> expressionEngine;

	static {
		INSTANCE = new CombinationFilterFactory();
	}

	protected DecimalFormat decimalFormat = new DecimalFormat( "#,##0.##" );
	protected DecimalFormat integerFormat = new DecimalFormat( "#,##0" );

	private CombinationFilterFactory() {
		expressionEngine = new PredicateExpressionParser<>();
		setupExpressionEngine();
	}

	public Predicate<List<Integer>> parse(String filterAsString) {
		return parse(filterAsString, false);
	}

	public Predicate<List<Integer>> parse(String filterAsString, boolean logFalseResults) {
		if (filterAsString == null || filterAsString.isEmpty()) {
			return numbers -> true;
		}
		Predicate<List<Integer>> filter = expressionEngine.processComplex(filterAsString.replace("\t", " ").replace("\n", "").replace("\r", ""), logFalseResults);
		return combo -> {
			Collections.sort(combo);
			return filter.test(combo);
		};
	}


	private void setupExpressionEngine() {
		expressionEngine.addSimpleExpression(
			expression ->
				expression.contains("emainder"),
			expression ->
				paramters ->
				buildPredicate(expression, this::buildRemainderFilter, (boolean)paramters[0])
		);
		expressionEngine.addSimpleExpression(
			expression ->
				expression.contains("sameLastDigit"),
			expression ->
				paramters ->
					buildPredicate(expression, this::buildSameLastDigitFilter, (boolean)paramters[0])
		);
		expressionEngine.addSimpleExpression(
			expression ->
				expression.contains("consecutiveLastDigit"),
			expression ->
				paramters ->
				buildPredicate(expression, this::buildConsecutiveLastDigitFilter, (boolean)paramters[0])
		);
		expressionEngine.addSimpleExpression(
			expression ->
				expression.contains("consecutiveNumber"),
			expression ->
				paramters ->
				buildPredicate(expression, this::buildConsecutiveNumberFilter, (boolean)paramters[0])
		);
		expressionEngine.addSimpleExpression(
			expression ->
				expression.contains("radius"),
			expression ->
				paramters ->
					buildPredicate(expression, this::buildRadiusFilter, (boolean)paramters[0])
		);
		expressionEngine.addSimpleExpression(
			expression ->
				expression.contains("sumOfPower"),
			expression ->
				paramters ->
					buildPredicate(expression, this::buildSumOfPowerFilter, (boolean)paramters[0])
		);
		expressionEngine.addSimpleExpression(
			expression ->
				expression.contains("sum"),
			expression ->
				paramters ->
					buildPredicate(expression, this::buildSumFilter, (boolean)paramters[0])
		);
		expressionEngine.addSimpleExpression(
			expression ->
				expression.contains("in"),
			expression ->
				paramters ->
					buildPredicate(expression, this::inFilter, (boolean)paramters[0])
		);
		expressionEngine.addSimpleExpression(
			expression ->
				expression.contains("->"),
			expression ->
				paramters ->
					buildPredicate(expression, this::buildNumberGroupFilter, (boolean)paramters[0])
		);
		expressionEngine.addSimpleExpression(
			expression ->
				expression.contains("true"),
			expression ->
				paramters ->
					buildPredicate(expression, exp -> combo -> true, (boolean)paramters[0])
		);
		expressionEngine.addSimpleExpression(
			expression ->
				expression.contains("false"),
			expression ->
				paramters ->
					buildPredicate(expression, exp -> combo -> false, (boolean)paramters[0])
		);
	}


	private Predicate<List<Integer>> inFilter(
		String filterAsString
	) {
		String[] operationOptions = filterAsString.replaceAll("\\s+","").split("in");
		String[] options = operationOptions[1].split(":");
		List<Integer> numbers = Arrays.stream(options[0].split(",")).map(Integer::parseInt).collect(Collectors.toList());
		String[] boundsAsString = options[1].split(",");
		int[] bounds = {
			Integer.parseInt(boundsAsString[0]),
			Integer.parseInt(boundsAsString[1])
		};
		return combo -> {
			int counter = 0;
			for (Integer number : combo) {
				if (numbers.indexOf(number) > -1) {
					counter++;
				}
			}
			return counter >= bounds[0] && counter <= bounds[1];
		};
	}


	private Predicate<List<Integer>> buildSumFilter(String filterAsString) {
		String[] operationOptions = filterAsString.replaceAll("\\s+","").split("sum")[1].split(":");
		Set<Integer> numbers = retrieveNumbersFromOption(operationOptions[1], TreeSet::new);
		return combo ->
			numbers.contains(
				ComboHandler.sumValues(combo)
			);
	}

	private Predicate<List<Integer>> buildSumOfPowerFilter(String filterAsString) {
		String[] operationOptions = filterAsString.replaceAll("\\s+","").split("sumOfPower")[1].split(":");
		List<Integer> operationOptionNumber = retrieveNumbersFromOption(
			operationOptions[0], ArrayList::new
		);
		Integer exponent = operationOptionNumber.get(0);
		Set<Integer> numbers = retrieveNumbersFromOption(operationOptions[1], TreeSet::new);
		return combo ->
			numbers.contains(
					ComboHandler.sumPowerOfValues(combo, exponent)
			);

	}

	private <C extends Collection<Integer>> C retrieveNumbersFromOption(String numberOptions, Supplier<C> collectionBuilder) {
		Collection<Integer> system = collectionBuilder.get();
		if (numberOptions.replaceAll("\\s+","").isEmpty()) {
			return (C)system;
		}
		for (String sumAsString : numberOptions.split(",")) {
			String[] rangeOptions = sumAsString.split("->");
			if (rangeOptions.length > 1) {
				IntStream.rangeClosed(
					Integer.parseInt(rangeOptions[0]),
					Integer.parseInt(rangeOptions[1])).boxed().collect(Collectors.toCollection(() -> system)
				);
			} else {
				system.add(Integer.parseInt(sumAsString));
			}
		}
		return (C)system;
	}

	private Predicate<List<Integer>> buildRadiusFilter(String filterAsString) {
		String[] operationOptions = filterAsString.replaceAll("\\s+","").split("radius");
		String[] rangeOptions = operationOptions[0].split("->");
		Integer leftRangeBounds = rangeOptions.length > 1 ? Integer.parseInt(rangeOptions[0]) : null;
		Integer rightRangeBounds =  rangeOptions.length > 1 ? Integer.parseInt(rangeOptions[1]) : null;
		String[] options = operationOptions[1].split(":");
		String[] boundsAsString = options[1].split(",");
		int leftOffset = Integer.parseInt(options[0].split(",")[0]);
		int rightOffset = Integer.parseInt(options[0].split(",")[1]);
		int[] bounds = {
			Integer.parseInt(boundsAsString[0]),
			Integer.parseInt(boundsAsString[1])
		};
		return combo -> {
			int maxNumbersInRange = 0;
			for (Integer number : combo) {
				if (rangeOptions.length > 1) {
					if (number > rightRangeBounds) {
						return true;
					} else if (number < leftRangeBounds) {
						continue;
					}
				}
				int numbersInRangeCounter = 0;
				int leftBound = number + leftOffset;
				int rightBound = number + rightOffset;
				for (Integer innNumber : combo) {
					if (number != innNumber && innNumber >= leftBound && innNumber <= rightBound) {
						if (numbersInRangeCounter == 0) {
							numbersInRangeCounter++;
						}
						if (++numbersInRangeCounter > bounds[1]) {
							return false;
						} else if (numbersInRangeCounter > maxNumbersInRange) {
							maxNumbersInRange = numbersInRangeCounter;
						}
					}
				}
				if (rangeOptions.length > 1 && numbersInRangeCounter < bounds[0]) {
					return false;
				}
			}
			return rangeOptions.length > 1 || maxNumbersInRange >= bounds[0];
		};
	}

	private Predicate<List<Integer>> buildConsecutiveNumberFilter(String filterAsString) {
		String[] operationOptions = filterAsString.replaceAll("\\s+","").split("consecutiveNumber");
		String[] rangeOptions = operationOptions[0].split("->");
		Integer leftRangeBounds = rangeOptions.length > 1 ? Integer.parseInt(rangeOptions[0]) : null;
		Integer rightRangeBounds =  rangeOptions.length > 1 ? Integer.parseInt(rangeOptions[1]) : null;
		String[] options = operationOptions[1].split(":");
		String[] boundsAsString = options[1].split(",");
		int[] bounds = {
			Integer.parseInt(boundsAsString[0]),
			Integer.parseInt(boundsAsString[1])
		};
		return combo -> {
			int counter = 0;
			int maxConsecutiveNumberCounter = 0;
			Integer previousNumber = null;
			for (int number : combo) {
				if (rangeOptions.length > 1) {
					if (number > rightRangeBounds) {
						break;
					} else if (number < leftRangeBounds) {
						continue;
					}
				}
				if (previousNumber != null && ((number != 0 && previousNumber == number -1) || (number == 0 && previousNumber == 9))) {
					if (counter == 0) {
						counter++;
					}
					if (++counter > maxConsecutiveNumberCounter) {
						maxConsecutiveNumberCounter = counter;
					}
				} else {
					counter = 0;
				}
				previousNumber = number;
			}
			return maxConsecutiveNumberCounter >= bounds[0] && maxConsecutiveNumberCounter <= bounds[1];
		};
	}

	private Predicate<List<Integer>> buildNumberGroupFilter(String filterAsString) {
		String[] expressions = filterAsString.split(";");
		int[][] bounds = new int[expressions.length][4];
		boolean allMinAreZeroTemp = true;
		for (int i = 0; i < expressions.length; i++) {
			String[] expression = expressions[i].replaceAll("\\s+","").split(":");
			String[] boundsAsString = expression[0].split("->");
			bounds[i][0] = Integer.parseInt(boundsAsString[0]);
			bounds[i][1] = Integer.parseInt(boundsAsString[1]);
			String[] values = expression[1].split(",");
			if ((bounds[i][2] = Integer.parseInt(values[0])) > 0) {
				allMinAreZeroTemp = false;
			}
			bounds[i][3] = Integer.parseInt(values[1]);
		}
		boolean allMinAreZero =  allMinAreZeroTemp;
		return combo -> {
			int[] checkCounter = new int[bounds.length];
			for (Integer number : combo) {
				for (int i = 0; i < bounds.length; i++) {
					if (number >= bounds[i][0] && number <= bounds[i][1] && ++checkCounter[i] > bounds[i][3]) {
						return false;
					}
				}
			}
			if (!allMinAreZero) {
				for (int i = 0; i < bounds.length; i++) {
					if (checkCounter[i] < bounds[i][2]) {
						return false;
					}
				}
			}
			return true;
		};
	}

	private Predicate<List<Integer>> buildRemainderFilter(String filterAsString) {
		String[] operationOptions = filterAsString.replaceAll("\\s+","").split("noRemainder|remainder");
		String[] rangeOptions = operationOptions[0].split("->");
		Integer leftRangeBounds = rangeOptions.length > 1 ?
			Integer.parseInt(rangeOptions[0]) : null;
		Integer rightRangeBounds =  rangeOptions.length > 1 ?
			Integer.parseInt(rangeOptions[1]) : null;
		String[] options = operationOptions[1].split(":");
		double divisor = options[0].isEmpty() ? 2 : Double.parseDouble(options[0]);
		String[] boundsAsString = options[1].split(",");
		int[] bounds = {
			Integer.parseInt(boundsAsString[0]),
			Integer.parseInt(boundsAsString[1])
		};
		DoublePredicate evenOrOddTester = filterAsString.contains("noRemainder") ?
			number -> number % divisor == 0 :
			number -> number % divisor != 0;
		return combo -> {
			int evenOrOddCounter = 0;
			for (Integer number : combo) {
				if (rangeOptions.length > 1) {
					if (number > rightRangeBounds) {
						break;
					} else if (number < leftRangeBounds) {
						continue;
					}
				}
				if(evenOrOddTester.test(number) && ++evenOrOddCounter > bounds[1]) {
					return false;
				}
			}
			return evenOrOddCounter >= bounds[0];
		};
	}

	private Predicate<List<Integer>> buildSameLastDigitFilter(String filterAsString) {
		String[] operationOptions = filterAsString.replaceAll("\\s+","").split("sameLastDigit");
		String[] rangeOptions = operationOptions[0].split("->");
		Integer leftRangeBounds = rangeOptions.length > 1 ? Integer.parseInt(rangeOptions[0]) : null;
		Integer rightRangeBounds =  rangeOptions.length > 1 ? Integer.parseInt(rangeOptions[1]) : null;
		String[] options = operationOptions[1].split(":");
		String[] boundsAsString = options[1].split(",");
		int[] bounds = {
			Integer.parseInt(boundsAsString[0]),
			Integer.parseInt(boundsAsString[1])
		};
		return combo -> {
			int[] counters = new int[10];
			for (Integer number : combo) {
				if (rangeOptions.length > 1) {
					if (number > rightRangeBounds) {
						break;
					} else if (number < leftRangeBounds) {
						continue;
					}
				}
				counters[(number % 10)]++;
			}
			int maxSameDigitCount = Arrays.stream(counters).summaryStatistics().getMax();
			return maxSameDigitCount >= bounds[0] && maxSameDigitCount <= bounds[1];
		};
	}

	private Predicate<List<Integer>> buildConsecutiveLastDigitFilter(String filterAsString) {
		String[] operationOptions = filterAsString.replaceAll("\\s+","").split("consecutiveLastDigit");
		String[] rangeOptions = operationOptions[0].split("->");
		Integer leftRangeBounds = rangeOptions.length > 1 ? Integer.parseInt(rangeOptions[0]) : null;
		Integer rightRangeBounds =  rangeOptions.length > 1 ? Integer.parseInt(rangeOptions[1]) : null;
		String[] options = operationOptions[1].split(":");
		String[] boundsAsString = options[1].split(",");
		int[] bounds = {
			Integer.parseInt(boundsAsString[0]),
			Integer.parseInt(boundsAsString[1])
		};
		return combo -> {
			Set<Integer> lastDigits = new TreeSet<>();
			for (Integer number : combo) {
				if (rangeOptions.length > 1) {
					if (number > rightRangeBounds) {
						break;
					} else if (number < leftRangeBounds) {
						continue;
					}
				}
				lastDigits.add(number % 10);
			}
			if (lastDigits.size() >= bounds[0] && lastDigits.size() <= bounds[1]) {
				return true;
			}
			int counter = 0;
			int maxConsecutiveLastDigitCounter = 0;
			Integer previousNumber = null;
			for (int number : lastDigits) {
				if (previousNumber != null && ((number != 0 && previousNumber == number -1) || (number == 0 && previousNumber == 9))) {
					if (counter == 0) {
						counter++;
					}
					if (++counter > maxConsecutiveLastDigitCounter) {
						maxConsecutiveLastDigitCounter = counter;
					}
				} else {
					counter = 0;
				}
				previousNumber = number;
			}
			return maxConsecutiveLastDigitCounter >= bounds[0] && maxConsecutiveLastDigitCounter <= bounds[1];
		};
	}

	private Predicate<List<Integer>> buildPredicate(
		String filterAsString,
		Function<String, Predicate<List<Integer>>> predicateBuilder,
		boolean logFalseResults
	) {
		Predicate<List<Integer>> predicate = predicateBuilder.apply(filterAsString);
		if (logFalseResults) {
			return combo -> {
				boolean result = predicate.test(combo);
				if (!result) {
					LogUtils.INSTANCE.info("[" + filterAsString + "] returned false on combo:\t" + ComboHandler.toString(combo));
				}
				return result;
			};
		}
		return predicate;
	}

}
