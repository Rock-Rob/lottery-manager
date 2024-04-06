package org.rg.game.lottery.engine;

import java.io.FileNotFoundException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.time.temporal.ChronoUnit;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.burningwave.Throwables;
import org.rg.game.core.CollectionUtils;
import org.rg.game.core.LogUtils;
import org.rg.game.core.NetworkUtils;
import org.rg.game.core.TimeUtils;

public abstract class LotteryMatrixGeneratorAbstEngine {

	private static final NumberProcessor numberProcessor;

	static {
		numberProcessor = new NumberProcessor();
	}

	protected int engineIndex;
	protected ProcessingContext processingContext;


	LotteryMatrixGeneratorAbstEngine() {
		engineIndex = getAllPreviousEngineAndConfigurations().size();
	}

	ProcessingContext getProcessingContext() {
		if (processingContext == null) {
			LogUtils.INSTANCE.warn("Warning: the setup method was never called on " + this.getClass().getSimpleName() + " " + engineIndex);
		}
		return processingContext;
	}

	public synchronized ProcessingContext setup(Properties config, boolean cacheEngineAndConfiguration) {
		ProcessingContext processingContext = this.processingContext = newProcessingContext();
		setupCombinationFilterPreProcessor();
		processingContext.comboSequencedIndexSelectorCounter = new AtomicInteger(0);
		processingContext.extractionArchiveStartDate = config.getProperty("competition.archive.start-date");
		processingContext.extractionArchiveForSeedStartDate = config.getProperty("seed-data.start-date");
		processingContext.seedShifter = Integer.valueOf(config.getProperty("seed-data.seed-shift", "0"));
		String extractionDatesAsString = config.getProperty("competition");
		Collection<LocalDate> extractionDates = computeExtractionDates(extractionDatesAsString);
		/*LogUtils.INSTANCE.info(
			"Computing for the following extraction dates:\n\t"+
			String.join(", ",
				extractionDates.stream().map(TimeUtils.defaultLocalDateFormat::format).collect(Collectors.toList())
			)
		);*/
		processingContext.storageType = config.getProperty("storage", "memory").replaceAll("\\s+","");
		String combinationCountConfigValue = config.getProperty("combination.count");
		if (combinationCountConfigValue != null && combinationCountConfigValue.replaceAll("\\s+","").equalsIgnoreCase("integral")) {
			config.remove("combination.filter");
			config.setProperty("combination.equilibrate", "false");
			config.setProperty("combination.selector", "sequence");
			config.setProperty("combination.count", combinationCountConfigValue = "-1");
		}
		processingContext.comboIndexSelectorType = config.getProperty("combination.selector", "random");
		String combinationFilterRaw = config.getProperty("combination.filter");
		String numbersProcessorConfigPrefix = Optional.ofNullable(
			config.getProperty("numbers-processor.config.prefix")
		).map(value -> value + ".").orElseGet(() -> "");
		processingContext.basicDataSupplier = extractionDate -> {
			if (processingContext.combinationFilter == null) {
				processingContext.combinationFilter = CombinationFilterFactory.INSTANCE.parse(
					preProcess(combinationFilterRaw, extractionDate)
				);
			}
			Map<String, Object> data = adjustSeed(extractionDate);
			NumberProcessor.Context<?> numberProcessorContext = new NumberProcessor.Context<>(
				getNumberGeneratorFactory(extractionDate), engineIndex, getAllPreviousEngineAndConfigurations()
			);
			List<Integer> chosenNumbers = numberProcessor.retrieveNumbersToBePlayed(
				numberProcessorContext,
				config.getProperty(numbersProcessorConfigPrefix + "numbers", getDefaultNumberRange()),
				extractionDate,
				CollectionUtils.INSTANCE.retrieveBoolean(
					config, numbersProcessorConfigPrefix + "numbers.ordered", "false"
				)
			);
			data.put("chosenNumbers", chosenNumbers);
			List<Integer> numbersToBePlayed = new ArrayList<>(chosenNumbers);
			data.put(
				"numbersToBeDiscarded",
				numberProcessor.retrieveNumbersToBeExcluded(
					numberProcessorContext,
					config.getProperty(numbersProcessorConfigPrefix + "numbers.discard"),
					extractionDate,
					numbersToBePlayed,
					CollectionUtils.INSTANCE.retrieveBoolean(config, numbersProcessorConfigPrefix + "numbers.ordered", "false")
				)
			);
			data.put("numbersToBePlayed", numbersToBePlayed);
			return data;
		};
		if (cacheEngineAndConfiguration) {
			try {
				getAllPreviousEngineAndConfigurations().get(engineIndex);
			} catch (IndexOutOfBoundsException exc) {
				Properties clonedConfig = new Properties();
				clonedConfig.putAll(config);
				getAllPreviousEngineAndConfigurations().add(
					new AbstractMap.SimpleEntry<>(
						newEngineBuilderWithId(this.engineIndex),
						() -> {
							Properties clonedConfigCopy = new Properties();
							clonedConfigCopy.putAll(clonedConfig);
							return clonedConfigCopy;
						}
					)
				);
			}
		}
		processingContext.reportEnabled = CollectionUtils.INSTANCE.retrieveBoolean(
			config,
			"report.enabled",
			Optional.ofNullable(System.getenv("report.enabled")).orElseGet(() -> "true")
		);
		processingContext.reportDetailEnabled = CollectionUtils.INSTANCE.retrieveBoolean(
			config,
			"report.detail.enabled",
			Optional.ofNullable(System.getenv("report.detail.enabled")).orElseGet(() -> "false")
		);
		String group = config.getProperty("group") != null ?
			config.getProperty("group").replace("${localhost.name}", NetworkUtils.INSTANCE.thisHostName()):
			null;
		processingContext.combinationFilterRaw = combinationFilterRaw;
		processingContext.testFilter = CollectionUtils.INSTANCE.retrieveBoolean(config, "combination.filter.test", "true");
		processingContext.testFilterFineInfo = CollectionUtils.INSTANCE.retrieveBoolean(config, "combination.filter.test.fine-info", "true");
		processingContext.combinationComponents = Integer.valueOf(config.getProperty("combination.components"));
		processingContext.occurrencesNumberRequested = Optional.ofNullable(config.getProperty("numbers.occurrences")).map(Double::parseDouble).orElseGet(() -> null);
		processingContext.numberOfCombosRequested = Optional.ofNullable(combinationCountConfigValue)
			.filter(value -> !value.replaceAll("\\s+","").isEmpty()).map(Integer::parseInt).orElseGet(() -> null);
		processingContext.chooseRandom = Integer.valueOf(config.getProperty("combination.choose-random.count", "0"));
		processingContext.equilibrateFlagSupplier = () -> {
			String equilibrateCombinations = config.getProperty("combination.equilibrate");
			if (equilibrateCombinations != null && !equilibrateCombinations.isEmpty() && !equilibrateCombinations.replaceAll("\\s+","").equalsIgnoreCase("random")) {
				return Boolean.parseBoolean(equilibrateCombinations);
			}
			return processingContext.random.nextBoolean();
		};
		processingContext.magicCombinationMinNumber = CollectionUtils.INSTANCE.retrieveBoolean(config, "combination.magic.enabled", "true") ?
			Integer.valueOf(Optional.ofNullable(config.getProperty("combination.magic.min-number")).orElseGet(() -> "1"))
			:null;
		processingContext.magicCombinationMaxNumber = CollectionUtils.INSTANCE.retrieveBoolean(config, "combination.magic.enabled", "true") ?
			Integer.valueOf(Optional.ofNullable(config.getProperty("combination.magic.max-number")).orElseGet(() -> "90"))
			:null;
		processingContext.group = group;
		processingContext.suffix = config.getProperty("nameSuffix");
		processingContext.notEquilibrateCombinationAtLeastOneNumberAmongThoseChosen = CollectionUtils.INSTANCE.retrieveBoolean(
			config,
			"combination.not-equilibrate.at-least-one-number-among-those-chosen",
			String.valueOf(!"sequence".equals(processingContext.comboIndexSelectorType))
		);
		processingContext.overwriteIfExists = Integer.parseInt(
			config.getProperty(
				"overwrite-if-exists",
				"1"
			)
		);
		processingContext.waitingSomeoneForGenerationTimeout = Integer.parseInt(
			config.getProperty(
				"waiting-someone-for-generation.timeout",
				"300"
			)
		);
		processingContext.executor = extractionDatePredicate -> storageProcessor -> {
			List<Storage> storages = new ArrayList<>();
			for (LocalDate extractionDate : extractionDates) {
				if (extractionDatePredicate != null) {
					Integer checkResult = extractionDatePredicate.apply(extractionDate).apply(storages);
					if (checkResult == 0 && storageProcessor != null) {
						storageProcessor.apply(extractionDate).accept(storages);
						continue;
					} else if (checkResult == -1) {
						continue;
					}
				}
				Storage storage = generate(processingContext, extractionDate);
				storages.add(
					storage
				);
				if (storageProcessor != null) {
					storageProcessor.apply(extractionDate).accept(storages);
				}
				LogUtils.INSTANCE.info();
				LogUtils.INSTANCE.info();
			}
			return storages;
		};
		String avoidModeConfigValue = config.getProperty("avoid", "never");
		if (avoidModeConfigValue.equals("never")) {
			processingContext.avoidMode = 0;
		} else if (avoidModeConfigValue.equals("if not suggested")) {
			processingContext.avoidMode = 1;
		} else if (avoidModeConfigValue.equals("if not strongly suggested")) {
			processingContext.avoidMode = 2;
		}
		return processingContext;
	}

	protected ProcessingContext newProcessingContext() {
		return new ProcessingContext(new DecimalFormat( "#,##0.##" ), new DecimalFormat( "#,##0" ), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
	}

	public Collection<LocalDate> computeExtractionDates(String extractionDatesAsString) {
		Collection<LocalDate> extractionDates = new LinkedHashSet<>();
		if (extractionDatesAsString == null || extractionDatesAsString.isEmpty()) {
			extractionDates.add(TimeUtils.today());
		} else {
			for(String expressionRaw : extractionDatesAsString.replaceAll("\\s+","").split(",")) {
				Collection<LocalDate> extractionDatesForExpression = new LinkedHashSet<>();
				String[] expressions = expressionRaw.split(":");
				Map<String, Object> nestedExpressionsData = new LinkedHashMap<>();
				String expression = PredicateExpressionParser.findAndReplaceNextBracketArea(expressions[0], nestedExpressionsData);
				expression = (String)nestedExpressionsData.getOrDefault(expression, expression);
				String[] dateWithOffset = expression.split("\\+");
				if ("thisWeek".equalsIgnoreCase(dateWithOffset[0])) {
					if (dateWithOffset.length == 2) {
						String[] range = dateWithOffset[1].split("\\*");
						if (range.length == 2) {
							for (int i = 0; i < Integer.parseInt(range[1]); i++) {
								extractionDatesForExpression.addAll(forNextWeek(Integer.parseInt(range[0])+i));
							}
						} else {
							extractionDatesForExpression.addAll(forNextWeek(Integer.valueOf(range[0])));
						}
					} else {
						extractionDatesForExpression.addAll(forThisWeek());
					}
				} else {
					LocalDate extractionDate = "next".equalsIgnoreCase(dateWithOffset[0])?
						computeNextExtractionDate(TimeUtils.today(), true) :
						computeNextExtractionDate(LocalDate.parse(dateWithOffset[0], TimeUtils.defaultLocalDateFormat), false);
					if (dateWithOffset.length == 2) {
						String[] range = dateWithOffset[1].split("\\*");
						for (int i = 0; i < Integer.parseInt(range[0]); i++) {
							extractionDate = extractionDate.plus(getIncrementDays(extractionDate, i == 0), ChronoUnit.DAYS);
						}
						if (range.length == 2) {
							for (int i = 0; i < Integer.parseInt(range[1]); i++) {
								extractionDatesForExpression.add(extractionDate);
								extractionDate = extractionDate.plus(getIncrementDays(extractionDate, i == 0), ChronoUnit.DAYS);
							}
						} else {
							extractionDatesForExpression.add(extractionDate);
						}
					} else {
						extractionDatesForExpression.add(extractionDate);
					}
				}
				if (expressions.length > 1) {
					Integer step = Integer.valueOf(expressions[1]);
					List<LocalDate> extractionDatesList = new ArrayList<>(extractionDatesForExpression);
					extractionDatesForExpression = new LinkedHashSet<>();
					for (int i = 0; i < extractionDatesList.size(); i++) {
						if ((i + 1) % step == 0) {
							extractionDatesForExpression.add(extractionDatesList.get(i));
						}
					}
				}
				extractionDates.addAll(extractionDatesForExpression);
			}
		}
		return extractionDates;
	}

	protected abstract String getDefaultNumberRange();

	public String preProcess(String filterAsString, LocalDate extractionDate) {
		return getProcessingContext().combinationFilterPreProcessor.preProcess(filterAsString, extractionDate);
	}

	protected void setupCombinationFilterPreProcessor() {
		getProcessingContext().combinationFilterPreProcessor = new PredicateExpressionParser<>();
		PredicateExpressionParser.PreProcessor preProcessor = getProcessingContext().combinationFilterPreProcessor.newPreProcessor();
		preProcessor.addSimpleExpression(
			expression ->
				expression.split("lessExtCouple|lessExt|mostExtCouple|mostExt").length > 1,
			expression ->
				parameters ->
					processStatsExpression(expression, (LocalDate)parameters[0])
		);
		preProcessor.addSimpleExpression(
			expression ->
				expression.contains("sum"),
			expression ->
				parameters ->
					processMathExpression(expression, (LocalDate)parameters[0])
		);
		preProcessor.addSimpleExpression(
			expression ->
				expression.contains("in"),
			expression ->
				parameters -> {
					return processInExpression(expression, (LocalDate)parameters[0]);
				}
		);
	}

	protected String processInExpression(String expression, LocalDate extractionDate) {
		throw new UnsupportedOperationException("Expression is not supported: " + expression);
	}

	protected String processMathExpression(String expression, LocalDate extractionDate) {
		throw new UnsupportedOperationException("Expression is not supported: " + expression);
	}

	protected String processStatsExpression(String expression, LocalDate extractionDate) {
		throw new UnsupportedOperationException("Expression is not supported: " + expression);
	}

	protected abstract Map<String, Object> testEffectiveness(String combinationFilterRaw, List<Integer> numbers, LocalDate extractionDate, boolean fineLog);

	private List<LocalDate> forThisWeek() {
		return forWeekOf(TimeUtils.today());
	}

	private List<LocalDate> forNextWeek() {
		return forNextWeek(1);
	}

	private List<LocalDate> forNextWeek(int offset) {
		return forWeekOf(TimeUtils.today().plus(offset, ChronoUnit.WEEKS));
	}

	private Storage buildStorage(
		LocalDate extractionDate,
		int combinationCount,
		int numberOfCombos,
		String group,
		String suffix
	) {
		if ("memory".equalsIgnoreCase(getProcessingContext().storageType)) {
			return new MemoryStorage(extractionDate, combinationCount, numberOfCombos, group, suffix);
		}
		return new PersistentStorage(extractionDate, combinationCount, numberOfCombos, group, suffix);
	}

	public Storage generate(
		ProcessingContext processingContext,
		LocalDate extractionDate
	) {
		Map<String, Object> data = processingContext.basicDataSupplier.apply(extractionDate);
		List<Integer> numbers = (List<Integer>)data.get("numbersToBePlayed");
		List<Integer> notSelectedNumbersToBePlayed = new ArrayList<>(numbers);
		Map<String, Object> effectivenessTestResults = null;
		if (processingContext.combinationFilterRaw != null && processingContext.testFilter) {
			effectivenessTestResults = testEffectiveness(
				processingContext.combinationFilterRaw,
				numbers,
				extractionDate,
				processingContext.testFilterFineInfo
			);
		}
		ComboHandler comboHandler = new ComboHandler(numbers, processingContext.combinationComponents);
		if (processingContext.numberOfCombosRequested != null && processingContext.numberOfCombosRequested.compareTo(-1) == 0) {
			processingContext.numberOfCombosRequested = comboHandler.getSizeAsInt();
		}
		Double ratio;
		Double occurrencesNumber = processingContext.occurrencesNumberRequested;
		Integer numberOfCombos = processingContext.numberOfCombosRequested;
		if (numberOfCombos != null) {
			ratio = (processingContext.combinationComponents * numberOfCombos) / (double)numbers.size();
		} else {
			ratio = occurrencesNumber;
			numberOfCombos = new BigDecimal((ratio * numbers.size()) / processingContext.combinationComponents).setScale(0, RoundingMode.UP).intValue();
		}
		Storage storageRef = null;
		if (processingContext.overwriteIfExists < 1 && "filesystem".equalsIgnoreCase(processingContext.storageType)) {
			storageRef = PersistentStorage.restore(processingContext.group, Storage.computeName(
				extractionDate, processingContext.combinationComponents, numberOfCombos, processingContext.suffix)
			);
			if (storageRef != null) {
				try {
					long timeout = processingContext.waitingSomeoneForGenerationTimeout * 1000;
					while (!storageRef.isClosed() && processingContext.overwriteIfExists == 0 && timeout >= 0) {
						try {
							LogUtils.INSTANCE.info("Waiting a maximum of " + timeout/1000 + " seconds for " + storageRef.getName() + " prepared by someone else");
							Thread.sleep(timeout - (timeout -= 1000));
						} catch (InterruptedException e) {
							Throwables.INSTANCE.throwException(e);
						}
					}
					if (storageRef.isClosed()) {
						LogUtils.INSTANCE.info(storageRef.getName() + " succesfully restored\n");
						storageRef.printAll();
						return storageRef;
					}
					if (processingContext.overwriteIfExists == -1) {
						LogUtils.INSTANCE.info(storageRef.getName() + " not generated");
						return null;
					}
					if (timeout < 0) {
						LogUtils.INSTANCE.info("Waiting for system generation by others ended: " + storageRef.getName() + " will be overwritten");
					}
				} catch (Throwable exc) {
					if (!(exc instanceof FileNotFoundException)) {
						throw exc;
					}
				}
			}
		}
		AtomicInteger discoveredComboCounter = new AtomicInteger(0);
		AtomicLong fromFilterDiscardedComboCounter = new AtomicLong(0);
		try (Storage storage = buildStorage(((LocalDate)data.get("seedStartDate")), processingContext.combinationComponents, numberOfCombos, processingContext.group, processingContext.suffix);) {
			storageRef = storage;
			boolean equilibrate = processingContext.equilibrateFlagSupplier.getAsBoolean();
			AtomicReference<Iterator<List<Integer>>> randomCombosIteratorWrapper = new AtomicReference<>();
			boolean[] alreadyComputed = new boolean[comboHandler.getSizeAsInt()];
			AtomicLong indexGeneratorCallsCounter = new AtomicLong(0L);
			AtomicInteger uniqueIndexCounter = new AtomicInteger(0);
			Integer ratioAsInt = null;
			//Integer remainder = null;
			try {
				if (equilibrate) {
					Map<Integer, AtomicInteger> occurrences = new LinkedHashMap<>();
					ratioAsInt = ratio.intValue();
					while (storage.size() < numberOfCombos) {
						List<Integer> underRatioNumbers = new ArrayList<>(numbers);
						List<Integer> overRatioNumbers = new ArrayList<>();
						for (Entry<Integer, AtomicInteger> entry : occurrences.entrySet()) {
							if (entry.getValue().get() >= ratioAsInt) {
								underRatioNumbers.remove(entry.getKey());
								if (entry.getValue().get() > ratioAsInt) {
									overRatioNumbers.add(entry.getKey());
								}
							}
						}
						List<Integer> selectedCombo;
						if (underRatioNumbers.size() < processingContext.combinationComponents) {
							do {
								selectedCombo = getNextCombo(
									randomCombosIteratorWrapper,
									comboHandler,
									alreadyComputed,
									indexGeneratorCallsCounter,
									uniqueIndexCounter,
									discoveredComboCounter,
									fromFilterDiscardedComboCounter
								);
							} while(selectedCombo == null || !selectedCombo.containsAll(underRatioNumbers) || containsOneOf(overRatioNumbers, selectedCombo));
							if (storage.addCombo(selectedCombo)) {
								incrementOccurences(occurrences, selectedCombo);
								notSelectedNumbersToBePlayed.removeAll(selectedCombo);
							}
						} else {
							do {
								selectedCombo = getNextCombo(
									randomCombosIteratorWrapper,
									comboHandler,
									alreadyComputed,
									indexGeneratorCallsCounter,
									uniqueIndexCounter,
									discoveredComboCounter,
									fromFilterDiscardedComboCounter
								);
							} while(selectedCombo == null);
							boolean canBeAdded = true;
							for (Integer number : selectedCombo) {
								AtomicInteger counter = occurrences.computeIfAbsent(number, key -> new AtomicInteger(0));
								if (counter.get() >= ratioAsInt) {
									canBeAdded = false;
									break;
								}
							}
							if (canBeAdded && storage.addCombo(selectedCombo)) {
								incrementOccurences(occurrences, selectedCombo);
								notSelectedNumbersToBePlayed.removeAll(selectedCombo);
							}
						}
					}
				} else {
					List<Integer> selectedCombo;
					for (int i = 0; i < numberOfCombos; i++) {
						do {
							selectedCombo = getNextCombo(
								randomCombosIteratorWrapper,
								comboHandler,
								alreadyComputed,
								indexGeneratorCallsCounter,
								uniqueIndexCounter,
								discoveredComboCounter,
								fromFilterDiscardedComboCounter
							);
						} while(selectedCombo == null);
						discoveredComboCounter.incrementAndGet();
						if (storage.addCombo(selectedCombo)) {
							notSelectedNumbersToBePlayed.removeAll(
								selectedCombo
						    );
						}
					}
					if (processingContext.notEquilibrateCombinationAtLeastOneNumberAmongThoseChosen) {
						while (!notSelectedNumbersToBePlayed.isEmpty()) {
							do {
								selectedCombo = getNextCombo(
									randomCombosIteratorWrapper,
									comboHandler,
									alreadyComputed,
									indexGeneratorCallsCounter,
									uniqueIndexCounter,
									discoveredComboCounter,
									fromFilterDiscardedComboCounter
								);
							} while(selectedCombo == null);
							discoveredComboCounter.incrementAndGet();
							List<Integer> numbersToBePlayedRemainedBeforeRemoving = new ArrayList<>(notSelectedNumbersToBePlayed);
							if (storage.addCombo(selectedCombo)) {
								notSelectedNumbersToBePlayed.removeAll(selectedCombo);
							} else {
								notSelectedNumbersToBePlayed = numbersToBePlayedRemainedBeforeRemoving;
							}
						}
					}
				}
			} catch (AllRandomNumbersHaveBeenGeneratedException exc) {
				LogUtils.INSTANCE.info();
				LogUtils.INSTANCE.info(exc.getMessage());
			}
			Integer minOccurrences = storage.getMinOccurence();
			Integer maxOccurrences = storage.getMaxOccurence();
			String systemGeneralInfo =
				"Per il concorso numero " + data.get("seed") + " del " + ((LocalDate)data.get("seedStartDate")).format(DateTimeFormatter.ofLocalizedDate(FormatStyle.LONG)) + " " +
				"il sistema " + (equilibrate ? "bilanciato " + (minOccurrences.compareTo(maxOccurrences) == 0 ? "perfetto " : "") +
				(minOccurrences.compareTo(maxOccurrences) == 0 ?
					"(occorrenza: " + minOccurrences :
					"(occorrenza minima: " + minOccurrences + ", occorrenza massima: " + maxOccurrences
				)+
				(processingContext.numberOfCombosRequested == null ? ", richiesta: " + processingContext.decimalFormat.format(processingContext.occurrencesNumberRequested) : "") + ") " : "") +
				"e' composto da " + processingContext.integerFormat.format(storage.size()) + " combinazioni " + "scelte su " + processingContext.integerFormat.format(comboHandler.getSizeAsInt()) + " totali" +
					(fromFilterDiscardedComboCounter.get() > 0 ? " (scartate dal filtro: " + processingContext.integerFormat.format(fromFilterDiscardedComboCounter.get()) + ")": "") +
				" e da " + numbers.size() + " numeri:\n" +  NumberProcessor.groupedForTenAsString(numbers, ", ", "\n") +
				(notSelectedNumbersToBePlayed.isEmpty() ? "" : "\nAttenzione: i seguenti numeri non sono stati inclusi nel sistema: " + NumberProcessor.toSimpleString(notSelectedNumbersToBePlayed))
			;
			boolean shouldBePlayed = processingContext.random.nextBoolean();
			boolean shouldBePlayedAbsolutely = processingContext.random.nextBoolean() && shouldBePlayed;
			if (processingContext.magicCombinationMinNumber != null && processingContext.magicCombinationMaxNumber != null) {
				Set<Integer> randomNumbers = new TreeSet<>();
				//Resettiamo il Random e simuliamo una generazione pulita
				processingContext.basicDataSupplier.apply(extractionDate);
				moveRandomizerToLast(processingContext.equilibrateFlagSupplier, discoveredComboCounter, comboHandler);
				Iterator<Integer> randomIntegers = processingContext.random.ints(processingContext.magicCombinationMinNumber, processingContext.magicCombinationMaxNumber + 1).iterator();
				while (randomNumbers.size() < processingContext.combinationComponents) {
					randomNumbers.add(randomIntegers.next());
				}
				List<Integer> randomCombo = new ArrayList<>();
				for (Integer number : randomNumbers) {
					randomCombo.add(number);
				}
				storage.addLine();
				storage.addLine("Combinazione magica:");
				storage.addUnindexedCombo(randomCombo);
			}
			int chooseRandom = processingContext.chooseRandom;
			if (chooseRandom > 0) {
				//Resettiamo il Random e simuliamo una generazione pulita
				processingContext.basicDataSupplier.apply(extractionDate);
				moveRandomizerToLast(processingContext.equilibrateFlagSupplier, discoveredComboCounter, comboHandler);
				Iterator<Integer> randomIntegers = processingContext.random.ints(0, storageRef.size()).iterator();
				storage.addLine();
				storage.addLine("Combinazioni scelte casualmente dal sistema:");
				while (chooseRandom > 0) {
					storage.addUnindexedCombo(storage.getCombo(randomIntegers.next()));
					chooseRandom--;
				}
			}
			storage.addLine(systemGeneralInfo);
			if (processingContext.reportEnabled) {
				Map<String, Object> report = checkQuality(storageRef, extractionDate);
				if (processingContext.reportDetailEnabled) {
					storage.addLine("\n");
					storage.addLine(
						(String)report.get("report.detail")
					);
				}
				storage.addLine("\n");
				storage.addLine(
					(String)report.get("report.summary")
				);
			}

			String text = "\n" +Storage.END_LINE_PREFIX + " " + (shouldBePlayedAbsolutely? "assolutamente " : "") + "di " + (shouldBePlayed? "giocare" : "non giocare") + " il sistema per questo concorso";
			storage.addLine(text);
			if (processingContext.avoidMode == 1 || processingContext.avoidMode == 2) {
				if ((processingContext.avoidMode == 1 && shouldBePlayed) || (processingContext.avoidMode == 2 && shouldBePlayedAbsolutely)) {
					storageRef.printAll();
				} else {
					LogUtils.INSTANCE.info(text);
					storageRef.delete();
				}
			} else {
				storageRef.printAll();
			}
		}
		return storageRef;
	}

	protected abstract Map<String, Object> checkQuality(Storage storageRef, LocalDate extractionDate);

	private List<Integer> getNextCombo(
		AtomicReference<Iterator<List<Integer>>> combosIteratorWrapper,
		ComboHandler comboHandler,
		boolean[] alreadyComputed,
		AtomicLong indexGeneratorCallsCounter,
		AtomicInteger uniqueIndexCounter,
		AtomicInteger discoveredComboCounter,
		AtomicLong fromFilterDiscardedComboCounter
	) {
		Iterator<List<Integer>> combosIterator = combosIteratorWrapper.get();
		List<Integer> selectedCombo;
		if (combosIterator != null && combosIterator.hasNext()) {
			selectedCombo = combosIterator.next();
		} else {
			combosIterator = getNextCombos(
				comboHandler,
				alreadyComputed,
				indexGeneratorCallsCounter,
				uniqueIndexCounter
			).iterator();
			combosIteratorWrapper.set(combosIterator);
			selectedCombo = combosIterator.next();
		}
		discoveredComboCounter.incrementAndGet();
		if (selectedCombo != null) {
			if (getProcessingContext().combinationFilter.test(selectedCombo)) {
				return selectedCombo;
			} else {
				fromFilterDiscardedComboCounter.incrementAndGet();
			}
		}
		return null;
	}

	private void moveRandomizerToLast(
		BooleanSupplier equilibrateFlagSupplier,
		AtomicInteger effectiveRandomCounter,
		ComboHandler comboHandler
	) {
		equilibrateFlagSupplier.getAsBoolean();
		long browsedCombo = effectiveRandomCounter.get();
		while (browsedCombo-- > 0) {
			getProcessingContext().random.nextInt(comboHandler.getSizeAsInt());
		}
	}

	private Map<String, Object> resetRandomizer(Supplier<List<Integer>> numberSupplier, LocalDate extractionDate) {
		Map<String, Object> data = adjustSeed(extractionDate);
		data.put("numbersToBePlayed", numberSupplier.get());
		return data;
	}

	private List<List<Integer>> getNextCombos(
		ComboHandler comboHandler,
		boolean[] alreadyComputed,
		AtomicLong indexGeneratorCallsCounter,
		AtomicInteger uniqueIndexCounter
	) {
		List<Long> effectiveIndexes = new ArrayList<>();
		Set<Long> indexesToBeProcessed = new HashSet<>();
		Integer size = comboHandler.getSizeAsInt();
		int randomCollSize = Math.min(size, 10_000_000);
		while (effectiveIndexes.size() < randomCollSize) {
			Integer idx = getProcessingContext().comboIndexSupplier.apply(size);
			indexGeneratorCallsCounter.incrementAndGet();
			if (!alreadyComputed[idx]) {
				Long idxAsLongValue = idx.longValue();
				effectiveIndexes.add(idxAsLongValue);
				indexesToBeProcessed.add(idxAsLongValue);
				uniqueIndexCounter.incrementAndGet();
				alreadyComputed[idx] = true;
			} else {
				effectiveIndexes.add(null);
			}
		}
		LogUtils.INSTANCE.info(
			getProcessingContext().formatter.format(TimeUtils.now()) +
			" - " + getProcessingContext().integerFormat.format(uniqueIndexCounter.get()) + " unique indexes generated on " +
			getProcessingContext().integerFormat.format(indexGeneratorCallsCounter.get()) + " calls. " +
			getProcessingContext().integerFormat.format(indexesToBeProcessed.size()) + " indexes will be processed in the current iteration."
		);
		if (size <= uniqueIndexCounter.get() && indexesToBeProcessed.isEmpty()) {
			throw new AllRandomNumbersHaveBeenGeneratedException();
		}
		Map<Long, List<Integer>> indexForCombos = comboHandler.find(indexesToBeProcessed, true);
		List<List<Integer>> combos = new ArrayList<>();
		for (Long index : effectiveIndexes) {
			List<Integer> combo = indexForCombos.get(index);
			combos.add(combo);
		}
		return combos;
	}

	protected boolean containsOneOf(List<Integer> overRatioNumbers, List<Integer> selectedCombo) {
		return overRatioNumbers.stream().anyMatch(element -> selectedCombo.contains(element));
	}

	protected void incrementOccurences(Map<Integer, AtomicInteger> occurrences, List<Integer> selectedCombo) {
		for (Integer number : selectedCombo) {
			AtomicInteger counter = occurrences.computeIfAbsent(number, key -> new AtomicInteger(0));
			counter.incrementAndGet();
		}
	}

	protected static String toString(List<Integer> numbers, int[] indexes) {
		return String.join(
			"\t",
			Arrays.stream(indexes)
			.map(numbers::get)
		    .mapToObj(String::valueOf)
		    .collect(Collectors.toList())
		);
	}

	//competition.archive.start-date
	public String getExtractionArchiveStartDate() {
		ProcessingContext processingContext = getProcessingContext();
		if (processingContext != null) {
			return processingContext.extractionArchiveStartDate != null ? processingContext.extractionArchiveStartDate : getDefaultExtractionArchiveStartDate();
		}
		return getDefaultExtractionArchiveStartDate();
	}

	//seed-data.start-date
	public String getExtractionArchiveForSeedStartDate() {
		ProcessingContext processingContext = getProcessingContext();
		if (processingContext != null) {
			return processingContext.extractionArchiveForSeedStartDate != null ? processingContext.extractionArchiveForSeedStartDate : getDefaultExtractionArchiveForSeedStartDate();
		}
		return getDefaultExtractionArchiveForSeedStartDate();
	}

	void buildComboIndexSupplier() {
		getProcessingContext().comboIndexSupplier = getProcessingContext().comboIndexSelectorType.equals("random") ?
			getProcessingContext().random::nextInt :
			getProcessingContext().comboIndexSelectorType.equals("sequence") ?
				this::nextSequencedIndex
				: null;
	}

	private Integer nextSequencedIndex(Integer size) {
		Integer index = getProcessingContext().comboSequencedIndexSelectorCounter.getAndIncrement();
		if (index >= size) {
			getProcessingContext().comboSequencedIndexSelectorCounter = new AtomicInteger(0);
			return nextSequencedIndex(size);
		}
		return index;
	}

	public abstract String getDefaultExtractionArchiveStartDate();

	protected abstract <E extends LotteryMatrixGeneratorAbstEngine> List<Entry<Supplier<E>, Supplier<Properties>>> getAllPreviousEngineAndConfigurations();

	protected abstract <E extends LotteryMatrixGeneratorAbstEngine> Supplier<E> newEngineBuilderWithId(int engineIndex);

	protected abstract List<LocalDate> forWeekOf(LocalDate dayOfWeek);

	public abstract Map<String, Object> adjustSeed(LocalDate extractionDate);

	public abstract LocalDate computeNextExtractionDate(LocalDate startDate, boolean incrementIfExpired);

	protected abstract int getIncrementDays(LocalDate startDate, boolean checkIfIsToday);

	protected abstract Function<String, Function<Integer, Function<Integer, Iterator<Integer>>>> getNumberGeneratorFactory(LocalDate extractionDate);

	public abstract String getDefaultExtractionArchiveForSeedStartDate();

	public static class ProcessingContext {
		DecimalFormat decimalFormat;
		DecimalFormat integerFormat;
		DateTimeFormatter formatter;
		Random random;
		Function<LocalDate, Map<String, Object>> basicDataSupplier;
		Integer seedShifter;
		private boolean reportEnabled;
		private boolean reportDetailEnabled;
		private Function<Integer, Integer> comboIndexSupplier;
		private String comboIndexSelectorType;
		private AtomicInteger comboSequencedIndexSelectorCounter;
		private String extractionArchiveStartDate;
		private String extractionArchiveForSeedStartDate;
		private String storageType;
		private Function<Function<LocalDate, Function<List<Storage>, Integer>>, Function<Function<LocalDate, Consumer<List<Storage>>>, List<Storage>>> executor;
		private Integer avoidMode;
		private Predicate<List<Integer>> combinationFilter;
		private PredicateExpressionParser<List<Integer>> combinationFilterPreProcessor;
		private String combinationFilterRaw;
		private boolean testFilter;
		private boolean testFilterFineInfo;
		private int combinationComponents;
		private Double occurrencesNumberRequested;
		private Integer numberOfCombosRequested;
		private int chooseRandom;
		private BooleanSupplier equilibrateFlagSupplier;
		private Integer magicCombinationMinNumber;
		private Integer magicCombinationMaxNumber;
		private String group;
		private String suffix;
		private boolean notEquilibrateCombinationAtLeastOneNumberAmongThoseChosen;
		private int overwriteIfExists;
		private int waitingSomeoneForGenerationTimeout;

		public ProcessingContext(
			DecimalFormat decimalFormat,
			DecimalFormat integerFormat,
			DateTimeFormatter formatter
		) {
			this.decimalFormat = decimalFormat;
			this.integerFormat = integerFormat;
			this.formatter = formatter;
		}

		public Function<Function<LocalDate, Function<List<Storage>, Integer>>, Function<Function<LocalDate, Consumer<List<Storage>>>, List<Storage>>> getExecutor() {
			return executor;
		}

		public List<Integer> computeNumbersToBePlayed(LocalDate extractionDate) {
			return (List<Integer>)basicDataSupplier.apply(extractionDate).get("numbersToBePlayed");
		}

	}

}

class AllRandomNumbersHaveBeenGeneratedException extends RuntimeException {

	private static final long serialVersionUID = 7009851378700603746L;

	public AllRandomNumbersHaveBeenGeneratedException() {
		super("All random numbers have been generated");
	}

}