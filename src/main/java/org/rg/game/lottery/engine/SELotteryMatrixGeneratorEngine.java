package org.rg.game.lottery.engine;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.rg.game.core.LogUtils;
import org.rg.game.core.TimeUtils;

public class SELotteryMatrixGeneratorEngine extends LotteryMatrixGeneratorAbstEngine {
	private static final List<Entry<Supplier<SELotteryMatrixGeneratorEngine>, Supplier<Properties>>> allPreviousEngineAndConfigurations;
	public final static SELotteryMatrixGeneratorEngine DEFAULT_INSTANCE;

	static {
		allPreviousEngineAndConfigurations = new ArrayList<>();
		DEFAULT_INSTANCE = new SELotteryMatrixGeneratorEngine() {

			{
				this.processingContext = newProcessingContext();
				setupCombinationFilterPreProcessor();
			}

			@Override
			public synchronized ProcessingContext setup(Properties config, boolean cacheEngineAndConfiguration) {
				throw new UnsupportedOperationException("Default instance cannot be initialized");
			}

			@Override
			public Storage generate(ProcessingContext pC, LocalDate extractionDate) {
				throw new UnsupportedOperationException("Default instance cannot generate systems");
			}

			@Override
			public Map<String, Object> testEffectiveness(
				String filter,
				List<Integer> numbers,
				LocalDate extractionDate,
				boolean fineLog
			) {
				return super.testEffectiveness(
					preProcess(filter, extractionDate),
					numbers,
					extractionDate,
					fineLog
				);
			}

		};
	}

	@Override
	public LocalDate computeNextExtractionDate(LocalDate startDate, boolean incrementIfExpired) {
		LocalDateTime now = TimeUtils.now();
		if (incrementIfExpired) {
			while (now.compareTo(
				SEStats.toClosingTime(startDate)
			) > 0) {
				startDate = startDate.plus(1, ChronoUnit.DAYS);
			}
		}
		SEStats sEStats = getSEStatsForSeed();
		int comparisonResult = TimeUtils.toDate(startDate).compareTo(sEStats.getLatestExtractionDate());
		if (comparisonResult >= 0) {
			if (comparisonResult == 0) {
				return startDate;
			}

			while (!SEStats.EXTRACTION_DAYS.contains(startDate.getDayOfWeek())) {
				startDate = startDate.plus(1, ChronoUnit.DAYS);
			}
		} else {
			List<Date> dates = new ArrayList<>(sEStats.getAllWinningCombosReversed().keySet());
			for (int i = 1; i < dates.size(); i++) {
				LocalDate extractionDate = TimeUtils.toLocalDate(dates.get(i));
				if (extractionDate.compareTo(startDate) >= 0) {
					return extractionDate;
				}
			}
		}
		return startDate;
	}

	@Override
	protected int getIncrementDays(LocalDate startDate, boolean checkIfIsToday) {
		SEStats sEStats = getSEStatsForSeed();
		int comparisonResult = TimeUtils.toDate(startDate).compareTo(sEStats.getLatestExtractionDate());
		if (comparisonResult < 0) {
			List<Date> dates = new ArrayList<>(sEStats.getAllWinningCombosReversed().keySet());
			for (int i = 1; i < dates.size(); i++) {
				LocalDate extractionDate = TimeUtils.toLocalDate(dates.get(i));
				if (extractionDate.compareTo(startDate) > 0) {
					return (int)TimeUtils.differenceInDays(startDate, extractionDate);
				}
			}
		}
		return SEStats.computeDaysToNextExtractionDate(startDate, checkIfIsToday);
	}

	@Override
	protected List<LocalDate> forWeekOf(LocalDate dayOfWeek) {
		LocalDate nextWeekStart = dayOfWeek.with(SEStats.EXTRACTION_DAYS.get(0));
		List<LocalDate> dates = new ArrayList<>();
		dates.add(nextWeekStart);
		dates.add(nextWeekStart.plus(getIncrementDays(nextWeekStart, false), ChronoUnit.DAYS));
		dates.add(dates.get(1).plus(getIncrementDays(nextWeekStart, false), ChronoUnit.DAYS));
		return dates;
	}

	@Override
	public Map<String, Object> adjustSeed(LocalDate extractionDate) {
		//Per il calcolo del seed prendiamo sempre l'istanza SEStats più aggiornata
		//In modo da avere le date corrette per i concorsi che hanno subito anticipi o posticipi
		Map.Entry<LocalDate, Long> seedRecord = getSEStatsForSeed().getSeedData(extractionDate);
		seedRecord.setValue(seedRecord.getValue() + getProcessingContext().seedShifter);
		getProcessingContext().random = new Random(seedRecord.getValue());
		buildComboIndexSupplier();
		Map<String, Object> seedData = new LinkedHashMap<>();
		seedData.put("seed", seedRecord.getValue());
		seedData.put("seedStartDate", seedRecord.getKey());
		return seedData;
	}


	@Override
	protected Function<String, Function<Integer, Function<Integer, Iterator<Integer>>>> getNumberGeneratorFactory(LocalDate extractionDate) {
		return generatorType-> leftBound -> rightBound -> {
			if (NumberProcessor.RANDOM_KEY.equals(generatorType)) {
				return getProcessingContext().random.ints(leftBound , rightBound + 1).iterator();
			} else if (NumberProcessor.MOST_EXTRACTED_KEY.equals(generatorType)) {
				return new BoundedIterator(getSEStats(extractionDate).getExtractedNumberRank(), leftBound, rightBound);
			} else if (NumberProcessor.MOST_EXTRACTED_COUPLE_KEY.equals(generatorType)) {
				return new BoundedIterator(getSEStats(extractionDate).getExtractedNumberFromMostExtractedCoupleRank(), leftBound, rightBound);
			} else if (NumberProcessor.MOST_EXTRACTED_TRIPLE_KEY.equals(generatorType)) {
				return new BoundedIterator(getSEStats(extractionDate).getExtractedNumberFromMostExtractedTripleRank(), leftBound, rightBound);
			} else if (NumberProcessor.LESS_EXTRACTED_KEY.equals(generatorType)) {
				return new BoundedIterator(getSEStats(extractionDate).getExtractedNumberRankReversed(), leftBound, rightBound);
			} else if (NumberProcessor.LESS_EXTRACTED_COUPLE_KEY.equals(generatorType)) {
				return new BoundedIterator(getSEStats(extractionDate).getExtractedNumberFromMostExtractedCoupleRankReversed(), leftBound, rightBound);
			} else if (NumberProcessor.LESS_EXTRACTED_TRIPLE_KEY.equals(generatorType)) {
				return new BoundedIterator(getSEStats(extractionDate).getExtractedNumberFromMostExtractedTripleRankReversed(), leftBound, rightBound);
			} else if (NumberProcessor.NEAREST_FROM_RECORD_ABSENCE_PERCENTAGE_KEY.equals(generatorType)) {
				return new BoundedIterator(getSEStats(extractionDate).getDistanceFromAbsenceRecordPercentageRankReversed(), leftBound, rightBound);
			} else if (NumberProcessor.BIGGEST_ABSENCE_RECORD_KEY.equals(generatorType)) {
				return new BoundedIterator(getSEStats(extractionDate).getAbsencesRecordFromCompetitionsRank(), leftBound, rightBound);
			} else if (NumberProcessor.SMALLEST_ABSENCE_RECORD_KEY.equals(generatorType)) {
				return new BoundedIterator(getSEStats(extractionDate).getAbsencesRecordFromCompetitionsRankReversed(), leftBound, rightBound);
			}
			throw new IllegalArgumentException("Unvalid generator type");
		};
	}

	@Override
	public String getDefaultExtractionArchiveStartDate() {
		return SEStats.FIRST_EXTRACTION_DATE_WITH_NEW_MACHINE_AS_STRING;
	}

	@Override
	public String getDefaultExtractionArchiveForSeedStartDate() {
		return SEStats.FIRST_EXTRACTION_DATE_AS_STRING;
	}

	@Override
	public Map<String, Object> testEffectiveness(String filterAsString, List<Integer> numbers, LocalDate extractionDate, boolean fineLog) {
		filterAsString = preProcess(filterAsString, extractionDate);
		Predicate<List<Integer>> combinationFilter = CombinationFilterFactory.INSTANCE.parse(filterAsString, fineLog);
		Set<Entry<Date, List<Integer>>> allWinningCombos = getSEStats(extractionDate).getAllWinningCombos().entrySet();
		int discardedFromHistory = 0;
		LogUtils.INSTANCE.info("Starting filter analysis\n");
		Collection<Integer> comboSums = new TreeSet<>();
		for (Map.Entry<Date, List<Integer>> comboForDate : allWinningCombos) {
			if (!combinationFilter.test(comboForDate.getValue())) {
				discardedFromHistory++;
				if (fineLog) {
					comboSums.add(comboForDate.getValue().stream().mapToInt(Integer::intValue).sum());
					LogUtils.INSTANCE.info("  Filter discarded winning combo of " + TimeUtils.getDefaultDateFormat().format(comboForDate.getKey()) + ":  " +
						ComboHandler.toString(comboForDate.getValue()));
				}
			}
		}
		if (fineLog) {
			String comboExpression = ComboHandler.toExpression(comboSums);
			if (!comboExpression.isEmpty()) {
				LogUtils.INSTANCE.info("\n" + ComboHandler.toExpression(comboSums));
			}
			LogUtils.INSTANCE.info();
		}
		ComboHandler comboHandler = new ComboHandler(numbers, 6);
		Collection<Long> comboPartitionIndexes = new HashSet<>();
		int discardedFromIntegralSystem = 0;
		int elaborationUnitSize = 25_000_000;
		combinationFilter = CombinationFilterFactory.INSTANCE.parse(filterAsString);
		for (long i = 0 ; i < comboHandler.getSizeAsLong(); i++) {
			comboPartitionIndexes.add(i);
			if (comboPartitionIndexes.size() == elaborationUnitSize) {
				/*if (fineLog) {
					LogUtils.INSTANCE.logInfo("Loaded " + integerFormat.format(i + 1) + " of indexes");
				}*/
				for (List<Integer> combo : comboHandler.find(comboPartitionIndexes, true).values()) {
					if (!combinationFilter.test(combo)) {
						discardedFromIntegralSystem++;
					}
				}
				if (fineLog) {
					LogUtils.INSTANCE.info("Processed " + getProcessingContext().integerFormat.format(i + 1) + " of combos");
				}
			}
		}
		if (comboPartitionIndexes.size() > 0) {
			for (List<Integer> combo : comboHandler.find(comboPartitionIndexes, true).values()) {
				if (!combinationFilter.test(combo)) {
					discardedFromIntegralSystem++;
				}
			}
			if (fineLog && comboHandler.getSizeAsInt() >= elaborationUnitSize) {
				LogUtils.INSTANCE.info("Processed " + getProcessingContext().integerFormat.format(comboHandler.getSizeAsInt()) + " of combo");
			}
		}
		if (fineLog && discardedFromHistory > 0) {
			LogUtils.INSTANCE.info();
		}
		Map<String, Object> stats = new LinkedHashMap<>();
		double discardedPercentageFromHistory = (discardedFromHistory * 100) / (double)allWinningCombos.size();
		double maintainedPercentageFromHistory = 100d - discardedPercentageFromHistory;
		double discardedFromIntegralSystemPercentage = (discardedFromIntegralSystem * 100) / (double)comboHandler.getSizeAsInt();
		Double discardedFromHistoryEstimation =
			allWinningCombos.size() > 0 ?
				new BigDecimal(comboHandler.getSizeAsInt()).multiply(new BigDecimal(discardedFromHistory))
					.divide(new BigDecimal(allWinningCombos.size()), 2, RoundingMode.HALF_UP).doubleValue():
				Double.POSITIVE_INFINITY;
		Double maintainedFromHistoryEstimation =
			allWinningCombos.size() > 0 ?
				new BigDecimal(comboHandler.getSizeAsInt()).multiply(new BigDecimal(allWinningCombos.size() - discardedFromHistory))
					.divide(new BigDecimal(allWinningCombos.size()), 2, RoundingMode.HALF_DOWN).intValue():
				Double.POSITIVE_INFINITY;
		double effectiveness = (maintainedPercentageFromHistory + discardedFromIntegralSystemPercentage) / 2d;
		/*double effectiveness = ((discardedFromIntegralSystem - discardedFromHistoryEstimation) * 100d) /
				comboHandler.getSize();*/
		StringBuffer report = new StringBuffer();
		report.append("Total extractions analyzed:" + rightAlignedString(getProcessingContext().integerFormat.format(allWinningCombos.size()), 25) + "\n");
		report.append("Discarded winning combos:" + rightAlignedString(getProcessingContext().integerFormat.format(discardedFromHistory), 27) + "\n");
		report.append("Discarded winning combos percentage:" + rightAlignedString(getProcessingContext().decimalFormat.format(discardedPercentageFromHistory) + " %", 18) + "\n");
		report.append("Maintained winning combos percentage:" + rightAlignedString(getProcessingContext().decimalFormat.format(maintainedPercentageFromHistory) + " %", 17) + "\n");
		report.append("Estimated maintained winning combos:" + rightAlignedString(getProcessingContext().decimalFormat.format(maintainedFromHistoryEstimation), 16) + "\n");
		report.append("Integral system total combos:" + rightAlignedString(getProcessingContext().decimalFormat.format(comboHandler.getSizeAsInt()), 23) + "\n");
		report.append("Integral system discarded combos:" + rightAlignedString(getProcessingContext().decimalFormat.format(discardedFromIntegralSystem), 19) + "\n");
		report.append("Integral system discarded combos percentage:" + rightAlignedString(getProcessingContext().decimalFormat.format(discardedFromIntegralSystemPercentage) + " %", 10) + "\n");
		report.append("Effectiveness:" + rightAlignedString(getProcessingContext().decimalFormat.format(effectiveness) + " %", 40) +"\n");
		LogUtils.INSTANCE.info(report.toString() + "\nFilter analysis ended\n");

		stats.put("totalExtractionsAnalyzed", allWinningCombos.size());
		stats.put("discardedWinningCombos", discardedFromHistory);
		stats.put("discardedWinningCombosPercentage", discardedPercentageFromHistory);
		stats.put("maintainedWinningCombosPercentage", maintainedPercentageFromHistory);
		stats.put("estimatedMaintainedWinningCombos", maintainedFromHistoryEstimation);
		stats.put("integralSystemTotalCombos", comboHandler.getSizeAsInt());
		stats.put("integralSystemDiscardedCombos", discardedFromIntegralSystem);
		stats.put("integralSystemDiscardedCombosPercentage", discardedFromIntegralSystemPercentage);
		stats.put("report", report);
		return stats;
	}

	private String rightAlignedString(String value, int emptySpacesCount) {
		return String.format("%" + emptySpacesCount + "s", value);
	}

	@Override
	protected String processStatsExpression(String expression, LocalDate extractionDate) {
		String[] options = expression.replaceAll("\\s+","").split("lessExtCouple|lessExt|mostExtCouple|mostExt");
		List<Integer> numbersToBeTested = null;
		if (expression.contains("lessExtCouple")) {
			numbersToBeTested =
				getSEStats(extractionDate).getExtractedNumberFromMostExtractedCoupleRankReversed();
		} else if (expression.contains("lessExt")) {
			numbersToBeTested =
				getSEStats(extractionDate).getExtractedNumberRankReversed();
		} else if (expression.contains("mostExtCouple")) {
			numbersToBeTested =
				getSEStats(extractionDate).getExtractedNumberFromMostExtractedCoupleRank();
		} else if (expression.contains("mostExt")) {
			numbersToBeTested =
				getSEStats(extractionDate).getExtractedNumberRank();
		}
		String[] subRange = options[0].split("->");
		if (subRange.length == 2) {
			Integer leftBound = Integer.parseInt(subRange[0]);
			Integer rightBound = Integer.parseInt(subRange[1]);
			numbersToBeTested = numbersToBeTested.stream().filter(number -> number >= leftBound && number <= rightBound).collect(Collectors.toList());
		}
		String[] groupOptions = options[1].split(":");
		List<String> numbers = new ArrayList<>();
		if (groupOptions[0].contains("->")) {
			String[] bounds = groupOptions[0].split("->");
			for (int i = Integer.parseInt(bounds[0]); i <= Integer.parseInt(bounds[1]); i++) {
				numbers.add(numbersToBeTested.get(i - 1).toString());
			}
		} else if (groupOptions[0].contains(",")) {
			for (String index : groupOptions[0].split(",")) {
				numbers.add(numbersToBeTested.get(Integer.parseInt(index) - 1).toString());
			}
		}
		return "in " + String.join(",", numbers) + ": " + groupOptions[1];
	}

	@Override
	protected String processInExpression(String expression, LocalDate extractionDate) {
		String[] options = expression.replaceAll("\\s+","").split("inallWinningCombos");
		if (options.length > 1) {
			String[] groupOptions = options[1].split(":");
			List<String> inClauses = new ArrayList<>();
			for (List<Integer> winningCombo :getSEStats(extractionDate).getAllWinningCombos().values()) {
				inClauses.add("in " + ComboHandler.toString(winningCombo, ",") + ":" + groupOptions[1]);
			}
			if (inClauses.isEmpty()) {//Storico non disponibile: probabilmente la data di inizio e fine sono la stessa. In questo caso controllare il file di configurazione
				return "";
			}
			return "(" + String.join("|", inClauses) + ")";
		}
		return expression;
	}

	@Override
	protected String processMathExpression(String expression, LocalDate extractionDate) {
		String[] options = expression.split("allWinningCombos");
		if (options.length > 1) {
			options[1] = options[1].startsWith(".") ?
					options[1].replaceFirst("\\.", "") : options[1];
			String manipulatedExpression = null;
			if ((manipulatedExpression = processMathManipulationExpression(
				options[1], "sum", "rangeOfSum",
				operationOptionValue -> {
					Collection<Integer> sums = new TreeSet<>();
					getSEStats(extractionDate).getAllWinningCombos().values().stream().forEach(combo ->
						sums.add(ComboHandler.sumValues(combo))
					);
					return sums;
				}
			)) != null) {
				return manipulatedExpression;
			} else if ((manipulatedExpression = processMathManipulationExpression(
				options[1], "sumOfPower", "rangeOfSumPower",
				operationOptionValue -> {
					Collection<Integer> sums = new TreeSet<>();
					getSEStats(extractionDate).getAllWinningCombos().values().stream().forEach(combo -> {
						sums.add(ComboHandler.sumPowerOfValues(combo, operationOptionValue.get(0)));
					});
					return sums;
				}
			)) != null) {
				return manipulatedExpression;
			}
			throw new UnsupportedOperationException("Expression is not supported: " + expression);
		}
		return expression;
	}

	public SEStats getSEStats(LocalDate extractionDate) {
		SEStats sEStats = SEStats.get(
			getExtractionArchiveStartDate(),
			TimeUtils.defaultLocalDateFormat.format(extractionDate)
		);
		LocalDate today = TimeUtils.today();
		if (today.compareTo(extractionDate) >= 0) {
			Date latestExtractionDate = sEStats.getLatestExtractionDate();
			if (latestExtractionDate != null && TimeUtils.toLocalDate(latestExtractionDate).compareTo(extractionDate) == 0 &&
			    !(extractionDate.compareTo(today) > 0 ||
				    (extractionDate.compareTo(today) == 0 && TimeUtils.now().compareTo(
				    	SEStats.toAfterExtraction(extractionDate)
					) < 0)
			    )
			) {
				latestExtractionDate = sEStats.getLatestExtractionDate(2);
				if (latestExtractionDate != null) {
					sEStats = SEStats.get(getExtractionArchiveStartDate(), TimeUtils.getDefaultDateFormat().format(latestExtractionDate));
				} else {
					sEStats = SEStats.get(
						getExtractionArchiveStartDate(),
						LocalDate.parse(
							getExtractionArchiveStartDate(),  TimeUtils.defaultLocalDateFormat
						).minus(1, ChronoUnit.DAYS).format( TimeUtils.defaultLocalDateFormat)
					);
				}
			}
		}
		return sEStats;
	}

	public SEStats getSEStatsForSeed() {
		return SEStats.get(getExtractionArchiveForSeedStartDate(),  TimeUtils.defaultLocalDateFormat.format(TimeUtils.today()));
	}

	private String processMathManipulationExpression(
		String expressionNameWithOptions,
		String expressionNameToMatch,
		String rangedExpressionNameToMatch,
		Function<List<Integer>, Collection<Integer>> processor
	) {
		Pattern comboPowerExpPattern = Pattern.compile("(\\b" + expressionNameToMatch +"\\b|\\b" + rangedExpressionNameToMatch + "\\b)");
		Matcher comboPowerExpFinder = comboPowerExpPattern.matcher(expressionNameWithOptions);
		if (comboPowerExpFinder.find()) {
			String mathExpressionType = comboPowerExpFinder.group(1);
			String[] operationOptions = expressionNameWithOptions.split(comboPowerExpPattern.pattern());
			List<Integer> operationOptionValues = new ArrayList<>();
			if (operationOptions.length > 1) {
				operationOptionValues.addAll(Arrays.stream(operationOptions[1].replaceAll("\\s+","").split(",")).map(Integer::parseInt).collect(Collectors.toList()));
			}
			Collection<Integer> sums = processor.apply(operationOptionValues);
			if (sums.isEmpty()) {
				return "true";
			}
			if (mathExpressionType.contains(expressionNameToMatch)) {
				return expressionNameToMatch + ComboHandler.toString(operationOptionValues, ",") + ": " + String.join(",", sums.stream().map(Object::toString).collect(Collectors.toList()));
			} else if (mathExpressionType.contains(rangedExpressionNameToMatch)) {
				return expressionNameToMatch + ComboHandler.toString(
					operationOptionValues, ","
				) + ": " + sums.iterator().next() + " -> " + sums.stream().reduce((prev, next) -> next).orElse(null);
			}
		}
		return null;
	}

	@Override
	protected String getDefaultNumberRange() {
		return "1 -> 90";
	}

	@Override
	protected Map<String, Object> checkQuality(Storage storage, LocalDate extractionDate) {
		return getSEStats(extractionDate)
			.checkQuality(storage::iterator);
	}

	@Override
	protected List<Entry<Supplier<SELotteryMatrixGeneratorEngine>, Supplier<Properties>>> getAllPreviousEngineAndConfigurations() {
		return allPreviousEngineAndConfigurations;
	}

	@Override
	protected Supplier<SELotteryMatrixGeneratorEngine> newEngineBuilderWithId(int index) {
		return () -> {
			SELotteryMatrixGeneratorEngine engine = new SELotteryMatrixGeneratorEngine();
			engine.engineIndex = index;
			return engine;
		};
	}

}