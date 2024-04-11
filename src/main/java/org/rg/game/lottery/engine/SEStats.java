package org.rg.game.lottery.engine;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.ComparisonOperator;
import org.apache.poi.ss.usermodel.FillPatternType;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.burningwave.Throwables;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.rg.game.core.FirestoreWrapper;
import org.rg.game.core.IOUtils;
import org.rg.game.core.LogUtils;
import org.rg.game.core.MathUtils;
import org.rg.game.core.TimeUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.firestore.DocumentSnapshot;

public class SEStats {
	private static final Map<String, SEStats> CACHE;
	private static final int CACHE_MAX_SIZE;
	public static final String FIRST_EXTRACTION_DATE_AS_STRING = "03/12/1997";
	public static final LocalDate FIRST_EXTRACTION_LOCAL_DATE = LocalDate.parse(FIRST_EXTRACTION_DATE_AS_STRING, TimeUtils.defaultLocalDateFormat);
	public static final Date FIRST_EXTRACTION_DATE = TimeUtils.toDate(FIRST_EXTRACTION_LOCAL_DATE);

	public static final String FIRST_EXTRACTION_DATE_WITH_NEW_MACHINE_AS_STRING = "02/07/2009";
	public static final LocalDate FIRST_EXTRACTION_LOCAL_DATE_WITH_NEW_MACHINE = LocalDate.parse(FIRST_EXTRACTION_DATE_WITH_NEW_MACHINE_AS_STRING, TimeUtils.defaultLocalDateFormat);
	public static final Date FIRST_EXTRACTION_DATE_WITH_NEW_MACHINE = TimeUtils.toDate(FIRST_EXTRACTION_LOCAL_DATE_WITH_NEW_MACHINE);

	public static final List<Integer> NUMBERS = IntStream.range(1, 91).boxed().collect(Collectors.toList());

	private static boolean forceLoadingFromExcel;
	private static boolean loadingFromFirebaseEnabled;

	public final static List<DayOfWeek> EXTRACTION_DAYS;

	static {
		SEStats.forceLoadingFromExcel =
				Boolean.parseBoolean(System.getenv().getOrDefault("se-stats.force-loading-from-excel", "false"));
		SEStats.loadingFromFirebaseEnabled =
				Boolean.parseBoolean(System.getenv().getOrDefault("se-stats.loading-from-firebase-enabled", "true")) && FirestoreWrapper.get() != null;
		CACHE = new LinkedHashMap<>();
		CACHE_MAX_SIZE = Optional.ofNullable(System.getenv("se-stats.cache.max-size")).map(Integer::parseInt).orElseGet(() -> 100);
		EXTRACTION_DAYS = Stream.of(System.getenv().getOrDefault("se-stats.extraction-days", "TUESDAY,THURSDAY,SATURDAY")
			.replaceAll("\\s+","").toUpperCase().split(",")).map(DayOfWeek::valueOf).collect(Collectors.toList());
	}

	private boolean global;

	protected Date startDate;
	protected Date endDate;

	private List<Map.Entry<String, Integer>> extractedNumberPairCounters;
	private List<Map.Entry<String, Integer>> extractedNumberTripleCounters;
	private List<Map.Entry<Integer, Integer>> extractedNumberCountersFromMostExtractedCouple;
	private List<Map.Entry<Integer, Integer>> extractedNumberCountersFromMostExtractedTriple;
	private List<Map.Entry<String, Integer>> extractedNumberCounters;
	private List<Map.Entry<String, Integer>> counterOfAbsencesFromCompetitions;
	private List<Map.Entry<String, Integer>> absencesRecordFromCompetitions;
	private List<Map.Entry<String, Integer>> distanceFromAbsenceRecord;
	private List<Map.Entry<String, Double>> distanceFromAbsenceRecordPercentage;
	private Map<Date, List<Integer>> allWinningCombos;
	private Map<Date, List<Integer>> allWinningCombosWithJollyAndSuperstar;

	private SEStats(String startDate, String endDate) {
		init(startDate, endDate);
	}

	public static final SEStats get(String startDate, String endDate) {
		boolean isGlobal = false;
		LocalDate today = TimeUtils.today();
		if (LocalDate.parse(endDate, TimeUtils.defaultLocalDateFormat).compareTo(today) >= 0) {
			endDate = TimeUtils.defaultLocalDateFormat.format(today);
			isGlobal = startDate.equals(FIRST_EXTRACTION_DATE_AS_STRING) || startDate.equals(FIRST_EXTRACTION_DATE_WITH_NEW_MACHINE_AS_STRING);
		}
		String key = startDate+"->"+endDate;
		SEStats sEStats = CACHE.get(key);
		if (sEStats == null) {
			synchronized(CACHE) {
				sEStats = CACHE.get(key);
				if (sEStats == null) {
					if (CACHE.size() >= CACHE_MAX_SIZE) {
						clear();
					}
					sEStats = new SEStats(startDate, endDate);
					sEStats.global = isGlobal;
					CACHE.put(key, sEStats);
				}
			}
		}
		return sEStats;
	}

	public static void hardClear() {
		synchronized(CACHE) {
			CACHE.clear();
		}
	}

	public static void clear() {
		LogUtils.INSTANCE.info("Cleaning " + SEStats.class.getSimpleName() + " cache");
		synchronized(CACHE) {
			Iterator<Map.Entry<String, SEStats>> sEStatsIterator = CACHE.entrySet().iterator();
			while (sEStatsIterator.hasNext()) {
				Map.Entry<String, SEStats> sEStatsData = sEStatsIterator.next();
				if (!sEStatsData.getValue().global) {
					sEStatsIterator.remove();
				}
			}
		}
		LogUtils.INSTANCE.info(SEStats.class.getSimpleName() + " cache cleaned");
	}

	private void init(String startDate, String endDate) {
		this.startDate = buildDate(startDate);
		this.endDate = buildDate(endDate);
		this.allWinningCombos = new TreeMap<>(TimeUtils.reversedDateComparator);
		this.allWinningCombosWithJollyAndSuperstar = new TreeMap<>(TimeUtils.reversedDateComparator);
		Collection<DataLoader> dataLoaders = new ArrayList<>();
		dataLoaders.add(new FromGlobalSEStatsDataLoader(this.startDate, this.endDate, allWinningCombos, allWinningCombosWithJollyAndSuperstar));
		if (loadingFromFirebaseEnabled) {
			dataLoaders.add(new FromFirebaseSEStatsDataLoader(this.startDate, this.endDate, allWinningCombos, allWinningCombosWithJollyAndSuperstar));
		}
		dataLoaders.add(new InternetDataLoader(this.startDate, this.endDate, allWinningCombos, allWinningCombosWithJollyAndSuperstar));
		dataLoaders.add(new FromExcelDataLoader(this.startDate, this.endDate, allWinningCombos, allWinningCombosWithJollyAndSuperstar));
		Collection<DataStorer> dataStorers = new ArrayList<>();
		if ((startDate.equals(FIRST_EXTRACTION_DATE_AS_STRING) || startDate.equals(FIRST_EXTRACTION_DATE_WITH_NEW_MACHINE_AS_STRING)) && TimeUtils.getDefaultDateFormat().format(new Date()).equals(endDate)) {
			/*dataStorers.add(
				new ToExcelDataStorerV1()
			);*/
			dataStorers.add(new ToExcelDataStorerV2(this));
			if (FirestoreWrapper.get() != null) {
				dataStorers.add(new ToFirebaseDataStorer(this));
			}
		}
		boolean dataLoaded = false;
		Class<DataLoader> dataLoaderClass = null;
		for (DataLoader dataLoader : dataLoaders) {
			try {
				if (dataLoaded = dataLoader.load()) {
					dataLoaderClass = (Class<DataLoader>)dataLoader.getClass();
					break;
				}
			} catch (Throwable exc) {
				LogUtils.INSTANCE.warn(dataLoader.getClass() + " in unable to load extractions data: " + exc.getMessage());
			}
		}
		if (!dataLoaded) {
			throw new RuntimeException("Unable to load data");
		}

		loadStats();
		LogUtils.INSTANCE.info();
		LogUtils.INSTANCE.info(
			"All extraction data have been succesfully loaded for period " + startDate + " -> " + endDate +
			Optional.ofNullable(dataLoaderClass).map(loaderClass -> " by " + loaderClass.getSimpleName()).orElseGet(() -> "")
		);
		for (DataStorer dataStorer : dataStorers) {
			try {
				if (dataStorer.store()) {

				} else {
					LogUtils.INSTANCE.info(dataStorer.getClass() + " stored no data");
				}
			} catch (Throwable exc) {
				LogUtils.INSTANCE.error(exc);
				LogUtils.INSTANCE.info(dataStorer.getClass() + " is unable to store extractions data: " + exc.getMessage());
			}
		}
	}

	public Date getStartDate() {
		return startDate;
	}

	public Date getEndDate() {
		return endDate;
	}

	private Date buildDate(String dateAsString) {
		try {
			return TimeUtils.getDefaultDateFormat().parse(dateAsString);
		} catch (ParseException exc) {
			return Throwables.INSTANCE.throwException(exc);
		}
	}

	private Map<String, Integer> buildExtractedNumberPairCountersMap() {
		ComboHandler comboHandler = new ComboHandler(SEStats.NUMBERS, 2);
		Collection<List<Integer>> allCouples = comboHandler.find(LongStream.range(0, comboHandler.getSizeAsLong()).boxed().collect(Collectors.toList()), true).values();
		Map<String, Integer> extractedNumberPairCountersMap = allCouples.stream().map(couple ->
			String.join("-", couple.stream().map(Object::toString).collect(Collectors.toList()))
		).collect(Collectors.toMap(key -> key, value -> 0, (x, y) -> y, LinkedHashMap::new));
		return extractedNumberPairCountersMap;
	}

	private Map<String, Integer> buildExtractedNumberTripleCountersMap() {
		ComboHandler comboHandler = new ComboHandler(SEStats.NUMBERS, 3);
		Collection<List<Integer>> allTriples = comboHandler.find(LongStream.range(0, comboHandler.getSizeAsLong()).boxed().collect(Collectors.toList()), true).values();
		Map<String, Integer> extractedNumberPairCountersMap = allTriples.stream().map(couple ->
			String.join("-", couple.stream().map(Object::toString).collect(Collectors.toList()))
		).collect(Collectors.toMap(key -> key, value -> 0, (x, y) -> y, LinkedHashMap::new));
		return extractedNumberPairCountersMap;
	}

	private void loadStats() {
		Map<String, Integer> extractedNumberPairCountersMap = buildExtractedNumberPairCountersMap();
		Map<String, Integer> extractedNumberTripleCountersMap = buildExtractedNumberTripleCountersMap();
		Map<String, Integer> extractedNumberCountersMap = new LinkedHashMap<>();
		Map<String, Integer> counterOfAbsencesFromCompetitionsMap = new LinkedHashMap<>();
		Map<String, Integer> absencesRecordFromCompetitionsMap = new LinkedHashMap<>();
		NUMBERS.forEach(number -> {
			counterOfAbsencesFromCompetitionsMap.put(number.toString(), 0);
			absencesRecordFromCompetitionsMap.put(number.toString(), 0);
		});
		new TreeMap<>(allWinningCombos).entrySet().forEach(dateAndExtractedCombo -> {
			List<Integer> extractedCombo = dateAndExtractedCombo.getValue();
			extractedCombo.stream().forEach(number -> {
				Integer counter = extractedNumberCountersMap.computeIfAbsent(number.toString(), key -> 0);
				extractedNumberCountersMap.put(number.toString(), ++counter);
			});
			analyzeMultipleNumber(extractedNumberPairCountersMap, extractedCombo, 2);
			analyzeMultipleNumber(extractedNumberTripleCountersMap, extractedCombo, 3);
			extractedCombo.stream().forEach(extractedNumber -> {
				counterOfAbsencesFromCompetitionsMap.put(String.valueOf(extractedNumber), 0);
			});
			counterOfAbsencesFromCompetitionsMap.entrySet().stream()
			.filter(entry -> !extractedCombo.contains(Integer.valueOf(entry.getKey())))
			.forEach(entry -> {
				Integer latestMaxCounter = entry.getValue() + 1;
				entry.setValue(latestMaxCounter);
				Integer maxCounter = absencesRecordFromCompetitionsMap.get(entry.getKey());
				if (maxCounter == null || maxCounter < latestMaxCounter) {
					absencesRecordFromCompetitionsMap.put(entry.getKey(), latestMaxCounter);
				}
			});
		});

		Comparator<Map.Entry<?, Integer>> integerComparator = (c1, c2) -> c1.getValue().compareTo(c2.getValue());
		Comparator<Map.Entry<String, Integer>> doubleIntegerComparator = (itemOne, itemTwo) ->
		(itemOne.getValue() < itemTwo.getValue()) ? -1 :
			(itemOne.getValue().compareTo(itemTwo.getValue()) == 0) ?
				Integer.valueOf(itemOne.getKey()).compareTo(Integer.valueOf(itemTwo.getKey())) :
				1;
		Comparator<Map.Entry<String, Integer>> doubleIntegerComparatorReversed = (itemOne, itemTwo) ->
		(itemOne.getValue() < itemTwo.getValue()) ? 1 :
			(itemOne.getValue().compareTo(itemTwo.getValue()) == 0) ?
				Integer.valueOf(itemOne.getKey()).compareTo(Integer.valueOf(itemTwo.getKey())) :
				-1;

		Comparator<Map.Entry<String, Double>> integerDoubleComparator= (itemOne, itemTwo) -> {
			return (itemOne.getValue() < itemTwo.getValue()) ? -1 :
				(itemOne.getValue().compareTo(itemTwo.getValue()) == 0) ?
					Integer.valueOf(itemOne.getKey()).compareTo(Integer.valueOf(itemTwo.getKey())) :
					1;
		};
		extractedNumberTripleCounters = extractedNumberTripleCountersMap.entrySet().stream().sorted(integerComparator.reversed()).collect(Collectors.toList());
		extractedNumberPairCounters = extractedNumberPairCountersMap.entrySet().stream().sorted(integerComparator.reversed()).collect(Collectors.toList());
		//extractedNumberPairCounters.stream().forEach(entry -> LogUtils.INSTANCE.logInfo(String.join("\t", Arrays.asList(entry.getKey().split("-"))) + "\t" + entry.getValue()));
		extractedNumberCounters = extractedNumberCountersMap.entrySet().stream().sorted(integerComparator.reversed()).collect(Collectors.toList());
		//extractedNumberCounters.stream().forEach(entry -> LogUtils.INSTANCE.logInfo(String.join("\t", Arrays.asList(entry.getKey().split("-"))) + "\t" + entry.getValue()));
		Map<Integer, Integer> extractedNumberFromMostExtractedCoupleMap = new LinkedHashMap<>();
		extractedNumberPairCounters.stream().forEach(entry -> {
			for (Integer number : Arrays.stream(entry.getKey().split("-")).map(Integer::parseInt).collect(Collectors.toList())) {
				Integer counter = extractedNumberFromMostExtractedCoupleMap.computeIfAbsent(number, key -> 0);
				extractedNumberFromMostExtractedCoupleMap.put(number, counter + entry.getValue());
			}
		});
		extractedNumberCountersFromMostExtractedCouple =
			extractedNumberFromMostExtractedCoupleMap.entrySet().stream().sorted(integerComparator.reversed()).collect(Collectors.toList());

		Map<Integer, Integer> extractedNumberFromMostExtractedTripleMap = new LinkedHashMap<>();
		extractedNumberTripleCounters.stream().forEach(entry -> {
			for (Integer number : Arrays.stream(entry.getKey().split("-")).map(Integer::parseInt).collect(Collectors.toList())) {
				Integer counter = extractedNumberFromMostExtractedTripleMap.computeIfAbsent(number, key -> 0);
				extractedNumberFromMostExtractedTripleMap.put(number, counter + entry.getValue());
			}
		});
		extractedNumberCountersFromMostExtractedTriple =
				extractedNumberFromMostExtractedTripleMap.entrySet().stream().sorted(integerComparator.reversed()).collect(Collectors.toList());

		counterOfAbsencesFromCompetitions =
			counterOfAbsencesFromCompetitionsMap.entrySet().stream().sorted(doubleIntegerComparatorReversed).collect(Collectors.toList());
		absencesRecordFromCompetitions =
			absencesRecordFromCompetitionsMap.entrySet().stream().sorted(doubleIntegerComparator.reversed()).collect(Collectors.toList());
		Map<String, Integer> distanceFromAbsenceRecordMap = new LinkedHashMap<>();
		Map<String, Double> distanceFromAbsenceRecordPercentageMap = new LinkedHashMap<>();
		counterOfAbsencesFromCompetitions.stream().forEach(entry -> {
			Integer absenceRecord = absencesRecordFromCompetitionsMap.get(entry.getKey());
			Integer distanceFromRecord = entry.getValue() - absenceRecord;
			distanceFromAbsenceRecordMap.put(entry.getKey(), distanceFromRecord);
			distanceFromAbsenceRecordPercentageMap.put(entry.getKey(), (distanceFromRecord * 100) / (double)absenceRecord);
		});
		distanceFromAbsenceRecord = distanceFromAbsenceRecordMap.entrySet().stream().sorted(integerComparator).collect(Collectors.toList());
		distanceFromAbsenceRecordPercentage =
			distanceFromAbsenceRecordPercentageMap.entrySet().stream().sorted(integerDoubleComparator).collect(Collectors.toList());
	}

	private void analyzeMultipleNumber(
		Map<String, Integer> multipleNumbersCountersMap,
		List<Integer> extractedCombo,
		int size
	) {
		ComboHandler comboHandler = new ComboHandler(extractedCombo, size);
		Collection<List<Integer>> allCouples = comboHandler.find(LongStream.range(0, comboHandler.getSizeAsLong())
			.boxed().collect(Collectors.toList()), true).values();
		List<String> multiplceNumbersCounters = allCouples.stream().map(couple ->
			String.join("-", couple.stream().map(Object::toString).collect(Collectors.toList()))
		).collect(Collectors.toList());
		for (String item : multiplceNumbersCounters) {
			multipleNumbersCountersMap.put(item, multipleNumbersCountersMap.get(item) + 1);
		}
	}

	public Date getLatestExtractionDate() {
		return getLatestExtractionDate(1);
	}

	public Date getLatestExtractionDate(int level) {
		try {
			Iterator<Date> itr = allWinningCombos.keySet().iterator();
			Date latestDate = null;
			while (level-- > 0) {
				latestDate = itr.next();
			}
			return latestDate;
		} catch (NoSuchElementException exc) {

		}
		return null;
	}

	public List<Integer> getExtractedNumberFromMostExtractedCoupleRank() {
		return extractedNumberCountersFromMostExtractedCouple.stream().map(entry -> entry.getKey()).collect(Collectors.toList());
	}

	public List<Integer> getExtractedNumberFromMostExtractedCoupleRankReversed() {
		return toReversed(getExtractedNumberFromMostExtractedCoupleRank());
	}

	public List<Integer> getExtractedNumberFromMostExtractedTripleRank() {
		return extractedNumberCountersFromMostExtractedTriple.stream().map(entry -> entry.getKey()).collect(Collectors.toList());
	}

	public List<Integer> getExtractedNumberFromMostExtractedTripleRankReversed() {
		return toReversed(getExtractedNumberFromMostExtractedTripleRank());
	}

	public List<Integer> getMostAbsentNumbersRank() {
		return counterOfAbsencesFromCompetitions.stream().map(entry -> Integer.parseInt(entry.getKey())).collect(Collectors.toList());
	}

	public List<Integer> getMostAbsentNumbersRankReversed() {
		return toReversed(getMostAbsentNumbersRank());
	}

	public List<Integer> getExtractedNumberRank() {
		return extractedNumberCounters.stream().map(entry -> Integer.parseInt(entry.getKey())).collect(Collectors.toList());
	}

	public List<Integer> getExtractedNumberRankReversed() {
		return toReversed(getExtractedNumberRank());
	}

	public List<Integer> getCounterOfAbsencesFromCompetitionsRank() {
		return counterOfAbsencesFromCompetitions.stream().map(entry -> Integer.parseInt(entry.getKey())).collect(Collectors.toList());
	}

	public List<Integer> getCounterOfAbsencesFromCompetitionsRankReversed() {
		return toReversed(getCounterOfAbsencesFromCompetitionsRank());
	}

	public List<Integer> getDistanceFromAbsenceRecordRank() {
		return distanceFromAbsenceRecord.stream().map(entry -> Integer.parseInt(entry.getKey())).collect(Collectors.toList());
	}

	public List<Integer> getDistanceFromAbsenceRecordRankReversed() {
		return toReversed(getDistanceFromAbsenceRecordRank());
	}

	public List<Integer> getDistanceFromAbsenceRecordPercentageRank() {
		return distanceFromAbsenceRecordPercentage.stream().map(entry -> Integer.parseInt(entry.getKey())).collect(Collectors.toList());
	}

	public List<Integer> getDistanceFromAbsenceRecordPercentageRankReversed() {
		return toReversed(getDistanceFromAbsenceRecordPercentageRank());
	}

	public List<Integer> getAbsencesRecordFromCompetitionsRank() {
		return absencesRecordFromCompetitions.stream().map(entry -> Integer.parseInt(entry.getKey())).collect(Collectors.toList());
	}

	public List<Integer> getAbsencesRecordFromCompetitionsRankReversed() {
		return toReversed(getAbsencesRecordFromCompetitionsRank());
	}

	public Integer getCounterOfAbsencesFromCompetitionsFor(Object number) {
		return getStatFor(counterOfAbsencesFromCompetitions, number);
	}

	public Integer getAbsenceRecordFromCompetitionsFor(Object number) {
		return getStatFor(absencesRecordFromCompetitions, number);
	}

	public Integer getExtractedNumberCountersFromMostExtractedCoupleFor(Object number) {
		return getStatFor(extractedNumberCountersFromMostExtractedCouple, number);
	}

	public Integer getExtractedNumberCountersFromMostExtractedTripleFor(Object number) {
		return getStatFor(extractedNumberCountersFromMostExtractedTriple, number);
	}

	public Integer getDistanceFromAbsenceRecordFor(Object number) {
		return getStatFor(distanceFromAbsenceRecord, number);
	}

	public Double getDistanceFromAbsenceRecordPercentageFor(Object number) {
		return getStatFor(distanceFromAbsenceRecordPercentage, number);
	}

	public Map<Date, List<Integer>> getAllWinningCombos() {
		return allWinningCombos;
	}

	public Map<Date, List<Integer>> getAllWinningCombosReversed() {
		List<Map.Entry<Date, List<Integer>>> allWinningCombosReversed = this.allWinningCombos.entrySet().stream().collect(Collectors.toList());
		Collections.reverse(allWinningCombosReversed);
		return allWinningCombosReversed.stream().collect(
			Collectors.toMap(
				Map.Entry::getKey,
				Map.Entry::getValue,
				(x, y) -> y,
				LinkedHashMap::new
			)
		);
	}

	public List<Integer> getWinningComboOf(Date date) {
		return allWinningCombos.entrySet().stream().filter(entry ->
			TimeUtils.getDefaultDateFormat().format(entry.getKey()).equals(TimeUtils.getDefaultDateFormat().format(date))
		).map(Map.Entry::getValue).findAny().orElseGet(() -> null);
	}

	public List<Integer> getWinningComboOf(LocalDate date) {
		return getWinningComboOf(TimeUtils.toDate(date));
	}

	public List<Integer> getWinningComboWithJollyAndSuperstarOf(Date date) {
		return allWinningCombosWithJollyAndSuperstar.entrySet().stream().filter(entry ->
			TimeUtils.getDefaultDateFormat().format(entry.getKey()).equals(TimeUtils.getDefaultDateFormat().format(date))
		).map(Map.Entry::getValue).findAny().orElseGet(() -> null);
	}

	public List<Integer> getWinningComboWithJollyAndSuperstarOf(LocalDate date) {
		return getWinningComboWithJollyAndSuperstarOf(TimeUtils.toDate(date));
	}

	public Integer getJollyOf(Date date) {
		return getWinningComboWithJollyAndSuperstarOf(date).get(6);
	}

	public Integer getJollyOf(LocalDate date) {
		return getWinningComboWithJollyAndSuperstarOf(date).get(6);
	}

	private <N extends Number> List<N> toReversed(List<N> source) {
		List<N> reversed = new ArrayList<>(source);
		Collections.reverse(reversed);
		return reversed;
	}

	private <N extends Number> N getStatFor(Collection<?> stats, Object numberObject) {
		Integer number = numberObject instanceof String ?
			Integer.parseInt((String)numberObject) :
			(Integer)numberObject;
		for (Object obj : stats) {
			Map.Entry<?, Integer> extractionData = (Map.Entry<?, Integer>)obj;
			Object key = extractionData.getKey();
			Integer iteratedNumber = key instanceof String ?
				Integer.parseInt((String)key) :
				(Integer)key;
			if (iteratedNumber.equals(number)) {
				return (N)extractionData.getValue();
			}
		}
		return null;
	}

	public Map<Date, List<Integer>> getAllWinningCombosWithJollyAndSuperstar() {
		return allWinningCombosWithJollyAndSuperstar;
	}

	public Map<String, Integer> checkFor(LocalDate extractionDate, Supplier<Iterator<List<Integer>>> systemIteratorSupplier) {
		List<Integer> winningCombo = getWinningComboOf(extractionDate);
		Map<String, Integer> results = new TreeMap<>();
		if (winningCombo == null) {
			return results;
		}
		Integer jolly = getJollyOf(extractionDate);
		Iterator<List<Integer>> systemIterator = systemIteratorSupplier.get();
		while (systemIterator.hasNext()) {
			List<Integer> currentCombo = systemIterator.next();
			Number hit = 0;
			for (Integer currentNumber : currentCombo) {
				if (winningCombo.contains(currentNumber)) {
					hit = hit.intValue() + 1;
				}
			}
			if (hit.intValue() > 1) {
				if (hit.intValue() == Premium.TYPE_FIVE.intValue()) {
					if (currentCombo.contains(jolly)) {
						hit = Premium.TYPE_FIVE_PLUS;
					}
				}
				String premiumLabel = Premium.toLabel(hit);
				results.put(premiumLabel, results.computeIfAbsent(premiumLabel, label -> 0) + 1);
			}
		}
		return results;
	}

	public Map<String, Object> checkQuality(Supplier<Iterator<List<Integer>>> systemIteratorSupplier, Number[]... premiums) {
		return checkQuality(systemIteratorSupplier, startDate, endDate, premiums);
	}

	public Map<String, Object> checkQualityFrom(Supplier<Iterator<List<Integer>>> systemIteratorSupplier, Date startDate, Number[]... premiums) {
		return checkQuality(systemIteratorSupplier, startDate, endDate, premiums);
	}

	public Map<String, Object> checkQualityTo(Supplier<Iterator<List<Integer>>> systemIteratorSupplier, Date endDate, Number[]... premiums) {
		return checkQuality(systemIteratorSupplier, startDate, endDate, premiums);
	}

	public Map<String, Object> checkQuality(
		Supplier<Iterator<List<Integer>>> systemIteratorSupplier,
		Date startDate,
		Date endDate,
		Number[]... premiumsFilters
	) {
		List<Number> premiumsFilterList = new ArrayList<>();
		List<Number> premiumsFilterListForReport = new ArrayList<>();
		if (premiumsFilters == null || premiumsFilters.length == 0) {
			premiumsFilterList.addAll(Premium.allTypesList());
			premiumsFilterListForReport.addAll(Premium.allTypesList());
		} else if (premiumsFilters.length == 1) {
			premiumsFilterList.addAll(Arrays.asList(premiumsFilters[0]));
			premiumsFilterListForReport.addAll(Arrays.asList(premiumsFilters[0]));
		} else {
			premiumsFilterList.addAll(Arrays.asList(premiumsFilters[0]));
			premiumsFilterListForReport.addAll(Arrays.asList(premiumsFilters[1]));
		}

		Map<String, Object> data = new LinkedHashMap<>();
		Iterator<List<Integer>> systemItearator = systemIteratorSupplier.get();
		long systemSize = 0;
		while (systemItearator.hasNext()) {
			systemItearator.next();
			systemSize++;
		}
		Map<Map.Entry<Date, List<Integer>>, Map<Number, List<List<Integer>>>> winningsCombosData = new LinkedHashMap<>();
		Map<Map.Entry<Date, List<Integer>>, Map<Number, List<List<Integer>>>> winningsCombosDataForReport = new LinkedHashMap<>();
		List<Map.Entry<Date, List<Integer>>> allWinningCombosReversed = this.allWinningCombosWithJollyAndSuperstar.entrySet().stream().collect(Collectors.toList());
		Collections.reverse(allWinningCombosReversed);
		int processedExtractionDateCounter = 0;
		Date effectiveStartDate = null;
		Date effectiveEndDate = null;
		for (Map.Entry<Date, List<Integer>> winningComboInfo : allWinningCombosReversed) {
			Date referenceDate = winningComboInfo.getKey();
			if (referenceDate.compareTo(startDate) >= 0 && referenceDate.compareTo(endDate) <= 0) {
				if (effectiveStartDate == null) {
					effectiveStartDate = referenceDate;
				}
				++processedExtractionDateCounter;
				Map<Number, List<List<Integer>>> winningCombosForExtraction = new TreeMap<>(MathUtils.INSTANCE.numberComparator);
				Map<Number, List<List<Integer>>> winningCombosForExtractionForReport = new TreeMap<>(MathUtils.INSTANCE.numberComparator);
				List<Integer> winningCombo = winningComboInfo.getValue();
				Integer jolly = winningCombo.get(6);
				systemItearator = systemIteratorSupplier.get();
				while (systemItearator.hasNext()) {
					List<Integer> currentCombo = systemItearator.next();
					Number hit = (int)currentCombo.stream().filter(winningCombo.subList(0, 6)::contains).count();
					if (hit.intValue() > 1) {
						if (hit.intValue() == Premium.TYPE_FIVE.intValue()) {
							if (currentCombo.contains(jolly)) {
								hit = Premium.TYPE_FIVE_PLUS;
							}
						}
						if (premiumsFilterList.contains(hit)) {
							winningCombosForExtraction.computeIfAbsent(hit, ht -> new ArrayList<>()).add(currentCombo);
						}
						if (premiumsFilterListForReport.contains(hit)) {
							winningCombosForExtractionForReport.computeIfAbsent(hit, ht -> new ArrayList<>()).add(currentCombo);
						}
					}
				}
				if (!winningCombosForExtraction.isEmpty()) {
					winningsCombosData.put(new AbstractMap.SimpleEntry<>(winningComboInfo.getKey(), winningCombo), winningCombosForExtraction);
				}
				if (!winningCombosForExtractionForReport.isEmpty()) {
					winningsCombosDataForReport.put(new AbstractMap.SimpleEntry<>(winningComboInfo.getKey(), winningCombo), winningCombosForExtractionForReport);
				}
				effectiveEndDate = referenceDate;
			}
		}
		data.put("winningCombos", winningsCombosData);
		Map<Number, Integer> premiumCounters = new TreeMap<>(MathUtils.INSTANCE.numberComparator);
		Iterator<Map.Entry<Entry<Date, List<Integer>>, Map<Number, List<List<Integer>>>>> winningsCombosDataItr = winningsCombosData.entrySet().iterator();
		while (winningsCombosDataItr.hasNext()) {
			Map.Entry<Map.Entry<Date, List<Integer>>, Map<Number, List<List<Integer>>>> winningCombosInfo = winningsCombosDataItr.next();
			for (Map.Entry<Number, List<List<Integer>>> winningCombos : winningCombosInfo.getValue().entrySet()) {
				Integer counter = premiumCounters.computeIfAbsent(winningCombos.getKey(), key -> 0);
				premiumCounters.put(winningCombos.getKey(), counter + winningCombos.getValue().size());
			}
		}
		if (effectiveStartDate == null) {
			effectiveStartDate = startDate;
		}
		if (effectiveEndDate == null) {
			effectiveEndDate = endDate;
		}
		List<String> labelsForReport = Premium.toLabels(premiumsFilterListForReport);
		StringBuffer reportDetail = new StringBuffer(
			"Risultati storici" + (labelsForReport.containsAll(Premium.allLabelsList()) ? " " :  " (" + String.join(", ", labelsForReport) + ") ") + "dal " +
				TimeUtils.getDefaultDateFormat().format(
					effectiveStartDate
				) +
			" al " +
				TimeUtils.getDefaultDateFormat().format(
					effectiveEndDate
				) +
			":\n\n"
		);
		winningsCombosDataItr = winningsCombosDataForReport.entrySet().iterator();
		while (winningsCombosDataItr.hasNext()) {
			Map.Entry<Map.Entry<Date, List<Integer>>, Map<Number, List<List<Integer>>>> winningCombosInfo = winningsCombosDataItr.next();
			reportDetail.append("\t" + TimeUtils.getDefaultDateFormat().format(winningCombosInfo.getKey().getKey()) + " -> " +
				SEComboHandler.toString(winningCombosInfo.getKey().getValue(), ", ", " - ") + ":\n"
			);
			for (Map.Entry<Number, List<List<Integer>>> winningCombos : winningCombosInfo.getValue().entrySet()) {
				reportDetail.append("\t\t" + Premium.toLabel(winningCombos.getKey()) + ":\n");
				for (List<Integer> combo : winningCombos.getValue()) {
					reportDetail.append("\t\t\t" + ComboHandler.toString(combo) + "\n");
				}
			}
			if (winningsCombosDataItr.hasNext()) {
				reportDetail.append("\n");
			}
		}
		if (winningsCombosDataForReport.isEmpty()) {
			reportDetail.append("\tnessuna vincita");
		}
		data.put("report.detail", reportDetail.toString());
		reportDetail = new StringBuffer("");
		reportDetail.append(
			processedExtractionDateCounter > 0 ?
				"Riepilogo risultati storici sistema dal " + TimeUtils.getDefaultDateFormat().format(effectiveStartDate) +
					" al " + TimeUtils.getDefaultDateFormat().format(effectiveEndDate) + ":\n\n":
				"Riepilogo risultati storici sistema non disponibile\n"
		);
		Integer returns = 0;
		for (Map.Entry<Number, Integer> winningInfo : premiumCounters.entrySet()) {
			Number type = winningInfo.getKey();
			String label = Premium.toLabel(type);
			returns += premiumPrice(type) * winningInfo.getValue();
			reportDetail.append("\t" + label + ":" + rightAlignedString(MathUtils.INSTANCE.integerFormat.format(winningInfo.getValue()), 28 - label.length()) + "\n");
		}
		if (processedExtractionDateCounter > 0) {
			reportDetail.append("\n\tCosto storico:" + rightAlignedString(MathUtils.INSTANCE.integerFormat.format(processedExtractionDateCounter * systemSize), 15) + "€\n");
			reportDetail.append("\tRitorno storico:" + rightAlignedString(MathUtils.INSTANCE.integerFormat.format(returns), 13) + "€\n");
			reportDetail.append("\tRapporto:" + rightAlignedString(MathUtils.INSTANCE.decimalFormat.format(((returns * 100d) / (processedExtractionDateCounter * systemSize)) - 100d), 20) + "%\n");
		}
		data.put("report.summary", reportDetail.toString());
		data.put("premium.counters", premiumCounters);
		Date referenceDate = getLatestExtractionDate();
		if (referenceDate == null) {//Nel caso lo storico non abbia dati
			referenceDate = this.endDate.compareTo(this.startDate) >= 0 ? this.endDate : this.startDate;
		}
		data.put("referenceDate", TimeUtils.getDefaultDateFormat().format(referenceDate));
		data.put("processedExtractionDateCounter", processedExtractionDateCounter);
		return data;
	}

	public static Integer premiumPrice(String label) {
		for (Map.Entry<Number, String> premiumEntry : Premium.all().entrySet()) {
			if (premiumEntry.getValue().equals(label)) {
				return premiumPrice(premiumEntry.getKey());
			}
		}
		return null;
	}

	public static Integer premiumPrice(Number type) {
		return type.intValue() == Premium.TYPE_TWO.intValue() ? 5 :
			type.intValue() == Premium.TYPE_THREE.intValue() ? 25 :
				type.intValue() == Premium.TYPE_FOUR.intValue() ? 300 :
					type.doubleValue() == Premium.TYPE_FIVE_PLUS.doubleValue() ? 620000 :
						type.intValue() == Premium.TYPE_FIVE.intValue() ? 32000:
							type.intValue() == 6 ?
								10000000 :
								0;
	}

	public static String rightAlignedString(String value, int emptySpacesCount) {
		return String.format("%" + emptySpacesCount + "s", value);
	}


	private static interface DataLoader {

		public boolean load() throws Throwable;

		static abstract class Abst implements DataLoader{
			protected Date startDate;
			protected Date endDate;
			protected Map<Date, List<Integer>> allWinningCombos;
			protected Map<Date, List<Integer>> allWinningCombosWithJollyAndSuperstar;
			protected DateFormat dateFmt;

			Abst(
				Date startDate,
				Date endDate
			) {
				this(startDate, endDate, new TreeMap<>(TimeUtils.reversedDateComparator), new TreeMap<>(TimeUtils.reversedDateComparator));
			}

			Abst(
				Date startDate,
				Date endDate,
				Map<Date, List<Integer>> allWinningCombos,
				Map<Date, List<Integer>> allWinningCombosWithJollyAndSuperstar
			) {
				this.startDate = startDate;
				this.endDate = endDate;
				this.allWinningCombos = allWinningCombos;
				this.allWinningCombosWithJollyAndSuperstar = allWinningCombosWithJollyAndSuperstar;
				this.dateFmt = new SimpleDateFormat("yyyy dd MMMM", Locale.ITALY);
			}
		}

	}

	private static interface DataStorer {

		public boolean store() throws Throwable;

	}

	public static class InternetDataLoader extends org.rg.game.lottery.engine.SEStats.DataLoader.Abst {
		public static final String LATEST_WINNING_COMBO_URL = "https://www.gntn-pgd.it/gntn-info-web/rest/gioco/superenalotto/estrazioni/ultimoconcorso";
		public static final String EXTRACTIONS_ARCHIVE_URL = "https://www.superenalotto.net/estrazioni/${year}";

		InternetDataLoader(Date startDate, Date endDate) {
			super(startDate, endDate);
		}

		InternetDataLoader(
			Date startDate,
			Date endDate,
			Map<Date, List<Integer>> allWinningCombos,
			Map<Date, List<Integer>> allWinningCombosWithJollyAndSuperstar
		) {
			super(startDate, endDate, allWinningCombos, allWinningCombosWithJollyAndSuperstar);
		}

		public static Map.Entry<Date, List<Integer>> getLatestWinningCombo() {
			try {
				URL url = new URL(LATEST_WINNING_COMBO_URL);
				HttpURLConnection connection = (HttpURLConnection) url.openConnection();
				connection.setRequestProperty("accept", "application/json");
				InputStream responseStream = connection.getInputStream();
				Map<String,Object> data = new ObjectMapper().readValue(responseStream, Map.class);
				connection.disconnect();
				Date extractionDate = TimeUtils.toDate(TimeUtils.toLocalDate(new Date((Long)data.get("dataEstrazione"))).atStartOfDay().toLocalDate());
				Map<String,Object> winningComboData = (Map<String,Object>)data.get("combinazioneVincente");
				List<Integer> winningCombo = ((List<String>)winningComboData.get("estratti"))
					.stream().map(Integer::valueOf).collect(Collectors.toList());
				winningCombo.add(Integer.valueOf((String)winningComboData.get("numeroJolly")));
				winningCombo.add(Integer.valueOf((String)winningComboData.get("superstar")));
				return new AbstractMap.SimpleEntry<>(extractionDate, winningCombo);
			} catch (IOException exc) {
				return Throwables.INSTANCE.throwException(exc);
			}
		}

		@Override
		public boolean load() throws Throwable {
			if (forceLoadingFromExcel) {
				LogUtils.INSTANCE.info("Loading data from internet is disabled");
				return false;
			}
			Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(TimeUtils.DEFAULT_TIME_ZONE));
			Date currentDate = calendar.getTime();
			calendar.setTime(startDate);
			int startYear =  calendar.get(Calendar.YEAR);
			calendar.setTime(currentDate);
			int endYear = calendar.get(Calendar.YEAR);
			LogUtils.INSTANCE.info();
			Map.Entry<Date, List<Integer>> latestWinningCombo = getLatestWinningCombo();
			if (TimeUtils.isBetween(latestWinningCombo.getKey(), startDate, endDate)) {
				allWinningCombos.put(latestWinningCombo.getKey(), new ArrayList<>(latestWinningCombo.getValue().subList(0, 6)));
				allWinningCombosWithJollyAndSuperstar.put(latestWinningCombo.getKey(), latestWinningCombo.getValue());
			}
			for (int year : IntStream.range(startYear, (endYear + 1)).map(i -> (endYear + 1) - i + startYear - 1).toArray()) {
				LogUtils.INSTANCE.info("Loading all extraction data of " + year);
				Document doc = Jsoup.connect(EXTRACTIONS_ARCHIVE_URL.replace("${year}", String.valueOf(year))).get();
				Element table = doc.select("table[class=resultsTable table light]").first();
				Iterator<Element> itr = table.select("tr").iterator();
				while (itr.hasNext()) {
					Element tableRow = itr.next();
					Elements dateCell = tableRow.select("td[class=date m-w60 m-righty]");
					if (!dateCell.isEmpty()) {
						Date extractionDate = dateFmt.parse(year + dateCell.iterator().next().textNodes().get(0).text());
						if (extractionDate.compareTo(startDate) >= 0 && extractionDate.compareTo(endDate) <= 0) {
							//LogUtils.INSTANCE.info(TimeUtils.defaultLocalDateFormat.format(TimeUtils.defaultLocalDateFormat.parse(year + dateCell.iterator().next().textNodes().get(0).text())) + "\t");
							List<Integer> extractedCombo = new ArrayList<>();
							for (Element number : tableRow.select("ul[class=balls]").first().children()) {
								String numberAsString = number.text();
								Integer extractedNumber = Integer.parseInt(numberAsString);
								extractedCombo.add(extractedNumber);
								//LogUtils.INSTANCE.info(extractedNumber + "\t");
							}
							Collections.sort(extractedCombo);
							allWinningCombos.put(extractionDate, extractedCombo);
							List<Integer> extractedComboWithJollyAndSuperstar = new ArrayList<>(extractedCombo);
							extractedComboWithJollyAndSuperstar.add(Integer.parseInt(tableRow.select("li[class=jolly]").first().text()));
							Element superStarData = tableRow.select("li[class=superstar]").first();
							if (superStarData != null) {
								extractedComboWithJollyAndSuperstar.add(Integer.parseInt(superStarData.text()));
							}
							allWinningCombosWithJollyAndSuperstar.put(extractionDate, extractedComboWithJollyAndSuperstar);
						}
					}
				}
			}
			return true;
		}

	}

	private class FromGlobalSEStatsDataLoader extends DataLoader.Abst {

		FromGlobalSEStatsDataLoader(Date startDate, Date endDate) {
			super(startDate, endDate);
		}

		FromGlobalSEStatsDataLoader(
			Date startDate,
			Date endDate,
			Map<Date, List<Integer>> allWinningCombos,
			Map<Date, List<Integer>> allWinningCombosWithJollyAndSuperstar
		) {
			super(startDate, endDate, allWinningCombos, allWinningCombosWithJollyAndSuperstar);
		}

		@Override
		public boolean load() throws Throwable {
			LocalDate today = TimeUtils.today();
			if (TimeUtils.toLocalDate(startDate).compareTo(FIRST_EXTRACTION_LOCAL_DATE) == 0 &&
				TimeUtils.toLocalDate(endDate).compareTo(today) == 0
			) {
				return false;
			}
			SEStats sEStats = SEStats.get(FIRST_EXTRACTION_DATE_AS_STRING, TimeUtils.defaultLocalDateFormat.format(today));
			for (Map.Entry<Date, List<Integer>> winningComboInfo : sEStats.allWinningCombos.entrySet()) {
				if (winningComboInfo.getKey().compareTo(startDate) >= 0 && winningComboInfo.getKey().compareTo(endDate) <= 0) {
					this.allWinningCombos.put(winningComboInfo.getKey(), winningComboInfo.getValue());
				}
			}
			for (Map.Entry<Date, List<Integer>> winningComboInfo : sEStats.allWinningCombosWithJollyAndSuperstar.entrySet()) {
				if (winningComboInfo.getKey().compareTo(startDate) >= 0 && winningComboInfo.getKey().compareTo(endDate) <= 0) {
					this.allWinningCombosWithJollyAndSuperstar.put(winningComboInfo.getKey(), winningComboInfo.getValue());
				}
			}
			return true;
		}

	}

	private class FromFirebaseSEStatsDataLoader extends DataLoader.Abst {

		FromFirebaseSEStatsDataLoader(Date startDate, Date endDate) {
			super(startDate, endDate);
		}

		FromFirebaseSEStatsDataLoader(
			Date startDate,
			Date endDate,
			Map<Date, List<Integer>> allWinningCombos,
			Map<Date, List<Integer>> allWinningCombosWithJollyAndSuperstar
		) {
			super(startDate, endDate, allWinningCombos, allWinningCombosWithJollyAndSuperstar);
		}

		@Override
		public boolean load() throws Throwable {
			DocumentSnapshot documentSnapshot = FirestoreWrapper.get().load(ToFirebaseDataStorer.getPath(TimeUtils.getDefaultDateFormat().format(startDate)));
			Date endDateFromDB = documentSnapshot.getDate("endDate");
			if (endDateFromDB == null) {
				return false;
			}
			if (endDateFromDB.compareTo(this.endDate) < 0) {
				return false;
			}
			allWinningCombos.putAll(
				(Map<Date, List<Integer>>)IOUtils.INSTANCE.serializeAndDecode(documentSnapshot.getString("allWinningCombos"))
			);
			allWinningCombosWithJollyAndSuperstar.putAll(
				(Map<Date, List<Integer>>)IOUtils.INSTANCE.serializeAndDecode(documentSnapshot.getString("allWinningCombosWithJollyAndSuperstar"))
			);
			return true;
		}

	}

	private static class FromExcelDataLoader extends DataLoader.Abst {
		FromExcelDataLoader(Date startDate, Date endDate) {
			super(startDate, endDate);
		}

		FromExcelDataLoader(
			Date startDate,
			Date endDate,
			Map<Date, List<Integer>> allWinningCombos,
			Map<Date, List<Integer>> allWinningCombosWithJollyAndSuperstar
		) {
			super(startDate, endDate, allWinningCombos, allWinningCombosWithJollyAndSuperstar);
		}

		@Override
		public boolean load() throws Throwable {
			try (InputStream inputStream = new FileInputStream(PersistentStorage.buildWorkingPath() + File.separator + ToExcelDataStorerV2.getFileName(FIRST_EXTRACTION_DATE_AS_STRING));
				Workbook workbook = new XSSFWorkbook(inputStream);
			) {
				Sheet sheet = workbook.getSheet("Storico estrazioni");
				Iterator<Row> rowIterator = sheet.rowIterator();
				//Skipping header
				rowIterator.next();
				while (rowIterator.hasNext()) {
					Row row = rowIterator.next();
					Iterator<Cell> numberIterator = row.cellIterator();
					Date extractionDate = numberIterator.next().getDateCellValue();
					if (extractionDate.compareTo(startDate) >= 0 && extractionDate.compareTo(endDate) <= 0) {
						List<Integer> extractedCombo = new ArrayList<>();
						while (numberIterator.hasNext()) {
							Integer number = (int)numberIterator.next().getNumericCellValue();
							extractedCombo.add(number);
							if (extractedCombo.size() == 6) {
								break;
							}
						}
						Collections.sort(extractedCombo);
						allWinningCombos.put(extractionDate, extractedCombo);
						List<Integer> extractedComboWithJollyAndSuperstar = new ArrayList<>(extractedCombo);
						extractedComboWithJollyAndSuperstar.add((int)numberIterator.next().getNumericCellValue());
						if (numberIterator.hasNext()) {
							extractedComboWithJollyAndSuperstar.add((int)numberIterator.next().getNumericCellValue());
						}
						allWinningCombosWithJollyAndSuperstar.put(extractionDate, extractedComboWithJollyAndSuperstar);
					}
				}
			}
			return true;
		}
	}

	private class ToExcelDataStorerV1 implements DataStorer {

		private String getFileName() {
			return "[SE]" + TimeUtils.getDefaultDateFmtForFilePrefix().format(startDate) + " - Archivio estrazioni e statistiche v1.xlsx";
		}

		private String getFileName(String startDateFormatted) throws ParseException {
			return "[SE]" + TimeUtils.getDefaultDateFmtForFilePrefix().format(TimeUtils.getDefaultDateFormat().parse(startDateFormatted)) + " - Archivio estrazioni e statistiche v1.xlsx";
		}

		@Override
		public boolean store() throws Throwable {
			try (FileOutputStream outputStream = new FileOutputStream(PersistentStorage.buildWorkingPath() + File.separator +  getFileName());
				SimpleWorkbookTemplate template = new SimpleWorkbookTemplate(true);
			) {
				CellStyle defaultNumberCellStyle = template.getOrCreateStyle(
					HorizontalAlignment.CENTER,
					"0"
				);
				CellStyle yellowBackground = template.getOrCreateStyle(
					"yellowBackgroundStyle",
					defaultNumberCellStyle,
					FillPatternType.SOLID_FOREGROUND,
					IndexedColors.YELLOW
				);
				CellStyle redBackground = template.getOrCreateStyle(
					"redBackgroundStyle",
					defaultNumberCellStyle,
					FillPatternType.SOLID_FOREGROUND,
					IndexedColors.RED
				);

				Sheet sheet = template.getOrCreateSheet("Numeri più estratti", true);
				sheet.setColumnWidth(0, 25 * 112);
				sheet.setColumnWidth(1, 25 * 192);
				template.createHeader(true, Arrays.asList("Numero", "Conteggio estrazioni"));
				for (Map.Entry<String, Integer> extractionData : extractedNumberCounters) {
					template.addRow();
					template.addCell(Integer.parseInt(extractionData.getKey()), "0");
					template.addCell(extractionData.getValue(), "0");
				}
				sheet = template.getOrCreateSheet("Numeri più estratti per coppia", true);
				sheet.setColumnWidth(0, 25 * 112);
				sheet.setColumnWidth(1, 25 * 400);
				template.createHeader(true, Arrays.asList("Numero", "Conteggio presenze nelle coppie più estratte"));
				for (Map.Entry<Integer, Integer> extractionData : extractedNumberCountersFromMostExtractedCouple) {
					template.addRow();
					template.addCell(extractionData.getKey(), "0");
					template.addCell(extractionData.getValue(), "0");
				}
				sheet = template.getOrCreateSheet("Coppie più estratte", true);
				sheet.setColumnWidth(0, 25 * 112);
				sheet.setColumnWidth(1, 25 * 112);
				sheet.setColumnWidth(2, 25 * 208);
				template.createHeader(true, Arrays.asList("1° numero", "2° numero", "Conteggio estrazioni"));
				for (Map.Entry<String, Integer> extractedNumberPairCounter : extractedNumberPairCounters) {
					String[] numbers = extractedNumberPairCounter.getKey().split("-");
					template.addRow();
					template.addCell(Integer.parseInt(numbers[0]), "0");
					template.addCell(Integer.parseInt(numbers[1]), "0");
					template.addCell(extractedNumberPairCounter.getValue(), "0");
				}
				sheet = template.getOrCreateSheet("Numeri ritardatari", true);
				sheet.setColumnWidth(0, 25 * 112);
				sheet.setColumnWidth(1, 25 * 176);
				template.createHeader(true, Arrays.asList("Numero", "Conteggio assenze"));
				for (Map.Entry<String, Integer> extractionData : counterOfAbsencesFromCompetitions) {
					template.addRow();
					template.addCell(Integer.parseInt(extractionData.getKey()), "0");
					template.addCell(extractionData.getValue(), "0");
				}
				sheet = template.getOrCreateSheet("Numeri più frequenti", true);
				sheet.setColumnWidth(0, 25 * 112);
				sheet.setColumnWidth(1, 25 * 256);
				template.createHeader(true, Arrays.asList("Numero", "Conteggio assenze massime"));
				for (Map.Entry<String, Integer> extractionData : absencesRecordFromCompetitions) {
					template.addRow();
					template.addCell(Integer.parseInt(extractionData.getKey()), "0");
					template.addCell(extractionData.getValue(), "0");
				}
				sheet = template.getOrCreateSheet("Storico estrazioni", true);
				sheet.setColumnWidth(0, 25 * 112);
				sheet.setColumnWidth(1, 25 * 112);
				sheet.setColumnWidth(2, 25 * 112);
				sheet.setColumnWidth(3, 25 * 112);
				sheet.setColumnWidth(4, 25 * 112);
				sheet.setColumnWidth(5, 25 * 112);
				sheet.setColumnWidth(6, 25 * 112);
				sheet.setColumnWidth(7, 25 * 112);
				sheet.setColumnWidth(8, 25 * 112);
				template.createHeader(true, Arrays.asList("Data", "1° numero", "2° numero", "3° numero", "4° numero", "5° numero", "6° numero", "Jolly", "Superstar"));
				for (Map.Entry<Date, List<Integer>> extractionData : allWinningCombosWithJollyAndSuperstar.entrySet()) {
					template.addRow();
					template.addCell(extractionData.getKey()).getCellStyle().setAlignment(HorizontalAlignment.LEFT);
					List<Integer> extractedNumers = extractionData.getValue();
					for (int i = 0; i < extractedNumers.size(); i++) {
						Cell cell = template.addCell(extractedNumers.get(i), "0");
						if (i == 6) {
							cell.setCellStyle(yellowBackground);
						}
						if (i == 7) {
							cell.setCellStyle(redBackground);
						}
					}
				}
				template.getWorkbook().write(outputStream);
			}
			return true;
		}

	}

	private static class ToFirebaseDataStorer implements DataStorer {
		SEStats sEStats;
		private ToFirebaseDataStorer(SEStats sEStats) {
			this.sEStats = sEStats;
		}

		private String getPath() {
			return "SEStats/" + TimeUtils.getDefaultDateFmtForFilePrefix().format(sEStats.startDate) + " - Archivio estrazioni e statistiche";
		}

		private static String getPath(String startDateFormatted) throws ParseException {
			return "SEStats/" + TimeUtils.getDefaultDateFmtForFilePrefix().format(TimeUtils.getDefaultDateFormat().parse(startDateFormatted)) + " - Archivio estrazioni e statistiche";
		}

		@Override
		public boolean store() throws Throwable {
			DocumentSnapshot document = FirestoreWrapper.get().load(getPath());
			Date endDate = document.getDate("endDate");
			if (endDate == null || endDate.compareTo(sEStats.endDate) < 0) {
				Map<String, Object> recordAsRawValue = new LinkedHashMap<>();
				recordAsRawValue.put("startDate", sEStats.startDate);
				recordAsRawValue.put("endDate", sEStats.endDate);
				recordAsRawValue.put(
					"allWinningCombos",
					IOUtils.INSTANCE.serializeAndEncode(new LinkedHashMap<>(sEStats.allWinningCombos))
				);
				recordAsRawValue.put(
					"allWinningCombosWithJollyAndSuperstar",
					IOUtils.INSTANCE.serializeAndEncode(new LinkedHashMap<>(sEStats.allWinningCombosWithJollyAndSuperstar))
				);
				FirestoreWrapper.get().write(getPath(), recordAsRawValue);
			}
			return true;
		}
	}

	private static class ToExcelDataStorerV2 implements DataStorer {
		SEStats sEStats;
		private ToExcelDataStorerV2(SEStats sEStats) {
			this.sEStats = sEStats;
		}

		private String getFileName() {
			return "[SE]" + TimeUtils.getDefaultDateFmtForFilePrefix().format(sEStats.startDate) + " - Archivio estrazioni e statistiche v2.xlsx";
		}

		private static String getFileName(String startDateFormatted) throws ParseException {
			return "[SE]" + TimeUtils.getDefaultDateFmtForFilePrefix().format(TimeUtils.getDefaultDateFormat().parse(startDateFormatted)) + " - Archivio estrazioni e statistiche v2.xlsx";
		}

		@Override
		public boolean store() throws Throwable {
			try (FileOutputStream outputStream = new FileOutputStream(PersistentStorage.buildWorkingPath() + File.separator +  getFileName());
				SimpleWorkbookTemplate template = new SimpleWorkbookTemplate(true);
			) {
				CellStyle defaultNumberCellStyle = template.getOrCreateStyle(
					HorizontalAlignment.CENTER,
					"0"
				);
				CellStyle percentageNumberStyle = template.getOrCreateStyle(
					defaultNumberCellStyle,
					"0.00%"
				);
				CellStyle yellowBackground = template.getOrCreateStyle(
					"yellowBackground",
					defaultNumberCellStyle,
					FillPatternType.SOLID_FOREGROUND,
					IndexedColors.YELLOW
				);
				CellStyle redBackground = template.getOrCreateStyle(
					"redBackground",
					defaultNumberCellStyle,
					FillPatternType.SOLID_FOREGROUND,
					IndexedColors.RED
				);

				Sheet sheet = template.getOrCreateSheet("Statistiche per numero", true);
				sheet.setColumnWidth(0, 2702);
				sheet.setColumnWidth(1, 2929);
				sheet.setColumnWidth(2, 3982);
				sheet.setColumnWidth(3, 3982);
				sheet.setColumnWidth(4, 3697);
				sheet.setColumnWidth(5, 3441);
				sheet.setColumnWidth(6, 3953);
				sheet.setColumnWidth(7, 4181);
				template.createHeader(
					true,
					Arrays.asList(
						"Numero",
						"Conteggio estrazioni",
						"Conteggio presenze nelle coppie più estratte",
						"Conteggio presenze nelle triplette più estratte",
						"Conteggio assenze consecutive",
						"Record di assenze consecutive",
						"Distanza dal record di assenze consecutive",
						"Distanza in % dal record di assenze consecutive"
					)
				).setHeight((short)1152);
				for (Map.Entry<String, Integer> extractionData : sEStats.extractedNumberCounters) {
					template.addRow();
					template.addCell(Integer.parseInt(extractionData.getKey()), "0");
					template.addCell(extractionData.getValue(), "0");
					template.addCell(sEStats.getExtractedNumberCountersFromMostExtractedCoupleFor(extractionData.getKey()), "0");
					template.addCell(sEStats.getExtractedNumberCountersFromMostExtractedTripleFor(extractionData.getKey()), "0");
					template.addCell(sEStats.getCounterOfAbsencesFromCompetitionsFor(extractionData.getKey()), "0");
					template.addCell(sEStats.getAbsenceRecordFromCompetitionsFor(extractionData.getKey()), "0");
					template.addCell(sEStats.getDistanceFromAbsenceRecordFor(extractionData.getKey()), "0");
					Double distanceFromAbsenceRecordPerc = sEStats.getDistanceFromAbsenceRecordPercentageFor(extractionData.getKey());
					template.addCell(
							distanceFromAbsenceRecordPerc /100
					).setCellStyle(percentageNumberStyle);
				}
				template.addSheetConditionalFormatting(7, IndexedColors.YELLOW, ComparisonOperator.BETWEEN, "-20%", "-10%");
				template.addSheetConditionalFormatting(7, IndexedColors.RED, ComparisonOperator.GT, "-10%");
				template.setAutoFilter();

				sheet = template.getOrCreateSheet("Coppie più estratte", true);
				sheet.setColumnWidth(0, 3640);
				sheet.setColumnWidth(1, 3640);
				sheet.setColumnWidth(2,	3584);
				template.createHeader(true, Arrays.asList("1° numero", "2° numero", "Conteggio estrazioni")).setHeight((short)576);
				for (Map.Entry<String, Integer> extractedNumberPairCounter : sEStats.extractedNumberPairCounters) {
					String[] numbers = extractedNumberPairCounter.getKey().split("-");
					template.addRow();
					template.addCell(Integer.parseInt(numbers[0]), "0");
					template.addCell(Integer.parseInt(numbers[1]), "0");
					template.addCell(extractedNumberPairCounter.getValue(), "0");
				}
				template.setAutoFilter();
				sheet = template.getOrCreateSheet("Triplette più estratte", true);
				sheet.setColumnWidth(0, 3640);
				sheet.setColumnWidth(1, 3640);
				sheet.setColumnWidth(2, 3640);
				sheet.setColumnWidth(3,	3584);
				template.createHeader(true, Arrays.asList("1° numero", "2° numero", "3° numero", "Conteggio estrazioni")).setHeight((short)576);
				for (Map.Entry<String, Integer> extractedNumberTripleCounter : sEStats.extractedNumberTripleCounters) {
					String[] numbers = extractedNumberTripleCounter.getKey().split("-");
					template.addRow();
					template.addCell(Integer.parseInt(numbers[0]), "0");
					template.addCell(Integer.parseInt(numbers[1]), "0");
					template.addCell(Integer.parseInt(numbers[2]), "0");
					template.addCell(extractedNumberTripleCounter.getValue(), "0");
				}
				template.setAutoFilter();
				sheet = template.getOrCreateSheet("Storico estrazioni", true);
				sheet.setColumnWidth(0, 2702);
				sheet.setColumnWidth(1,	3640);
				sheet.setColumnWidth(2,	3640);
				sheet.setColumnWidth(3,	3640);
				sheet.setColumnWidth(4,	3640);
				sheet.setColumnWidth(5,	3640);
				sheet.setColumnWidth(6,	3640);
				sheet.setColumnWidth(7,	2332);
				sheet.setColumnWidth(8,	3441);
				template.createHeader(
					true,
					Arrays.asList("Data", "1° numero", "2° numero", "3° numero", "4° numero", "5° numero", "6° numero", "Jolly", "Superstar")
				).setHeight((short)288);
				for (Map.Entry<Date, List<Integer>> extractionData : sEStats.allWinningCombosWithJollyAndSuperstar.entrySet()) {
					template.addRow();
					template.addCell(extractionData.getKey()).getCellStyle().setAlignment(HorizontalAlignment.LEFT);
					List<Integer> extractedNumers = extractionData.getValue();
					for (int i = 0; i < extractedNumers.size(); i++) {
						Cell cell = template.addCell(extractedNumers.get(i), "0");
						if (i == 6) {
							cell.setCellStyle(yellowBackground);
						}
						if (i == 7) {
							cell.setCellStyle(redBackground);
						}
					}
				}
				template.setAutoFilter();
				template.getWorkbook().write(outputStream);
			}
			return true;
		}

	}

	public Map.Entry<LocalDate, Long> getSeedData(LocalDate extractionDate) {
		int size = allWinningCombos.size();
		long counter = 0;
		LocalDate seedStartDate = null;
		for (Map.Entry<Date, List<Integer>> extractionData : allWinningCombos.entrySet()) {
			seedStartDate = TimeUtils.toLocalDate(extractionData.getKey());
			if (seedStartDate.compareTo(extractionDate) <= 0) {
				break;
			}
			counter++;
		}
		if (counter > 0) {
			return new AbstractMap.SimpleEntry<>(seedStartDate, size - counter);
		} else {
			if (seedStartDate == null) {
				SEStats sEStats = SEStats.get(
					TimeUtils.getDefaultDateFormat().format(this.startDate),
					TimeUtils.getDefaultDateFormat().format(new Date())
				);
				if (!sEStats.allWinningCombos.isEmpty()) {
					return sEStats.getSeedData(extractionDate);
				} else {
					seedStartDate = TimeUtils.toLocalDate(this.startDate);
				}
			} else {
				counter = size;
			}
			while (seedStartDate.compareTo(extractionDate) < 0) {
				seedStartDate = seedStartDate.plus(computeDaysToNextExtractionDateConsideringNotFoundAsModified(seedStartDate), ChronoUnit.DAYS);
				counter++;
			}
		}
		return new AbstractMap.SimpleEntry<>(seedStartDate, counter);
	}

	public static int computeDaysToNextExtractionDateConsideringNotFoundAsModified(LocalDate startDate) {
		DayOfWeek currentDayOfWeek = startDate.getDayOfWeek();
		List<DayOfWeek> extractionDates = EXTRACTION_DAYS;
		if (!extractionDates.contains(currentDayOfWeek)) {
			LogUtils.INSTANCE.info("Attenzione: il concorso eseguito in data " + TimeUtils.defaultLocalDateWithDayNameFormat.format(startDate) + " risulta essere anticipato");
			extractionDates = new ArrayList<>(EXTRACTION_DAYS);
			DayOfWeek nextExctractionDay = null;
			for (DayOfWeek extractionDay : extractionDates) {
				if (currentDayOfWeek.compareTo(extractionDay) < 0) {
					nextExctractionDay = extractionDay;
					break;
				}
			}
			if (nextExctractionDay != null) {
				extractionDates.set(extractionDates.indexOf(nextExctractionDay), currentDayOfWeek);
			}
		}
		return computeDaysToNextExtractionDate(startDate, extractionDates, false);
	}


	public static int computeDaysToNextExtractionDate(LocalDate startDate, boolean checkIfIsToday) {
		return computeDaysToNextExtractionDate(startDate, EXTRACTION_DAYS, checkIfIsToday);
	}

	private static int computeDaysToNextExtractionDate(LocalDate startDate, List<DayOfWeek> extractionDates, boolean checkIfIsToday) {
		DayOfWeek currentDayOfWeek = startDate.getDayOfWeek();
		DayOfWeek nextExctractionDay = null;
		boolean isExtractionDate = false;
		LocalDateTime now = TimeUtils.now();
		for (DayOfWeek extractionDay : extractionDates) {
			if (currentDayOfWeek.compareTo(extractionDay) == 0) {
				isExtractionDate = true;
			}
			if (currentDayOfWeek.compareTo(extractionDay) < 0 ||
				(checkIfIsToday && currentDayOfWeek.compareTo(extractionDay) == 0 && now.getDayOfWeek().compareTo(extractionDay) == 0 && now.compareTo(SEStats.toClosingTime(now)) < 0))
			{
				nextExctractionDay = extractionDay;
				break;
			}
		}
		if (nextExctractionDay != null) {
			return nextExctractionDay.ordinal() - currentDayOfWeek.ordinal();
		} else {
			if (checkIfIsToday) {
				if (now.toLocalDate().compareTo(startDate) == 0 && isExtractionDate && now.compareTo(SEStats.toClosingTime(now)) < 0) {
					return 0;
				}
			}
			return (DayOfWeek.SUNDAY.ordinal() - currentDayOfWeek.ordinal()) + extractionDates.get(0).getValue();
		}
	}

	public static int computeDaysFromPreviousExtractionDate(LocalDate startDate) {
		return computeDaysFromPreviousExtractionDate(startDate, false);
	}

	public static int computeDaysFromPreviousExtractionDate(LocalDate startDate, boolean checkIfIsToday) {
		return computeDaysFromPreviousExtractionDate(startDate, EXTRACTION_DAYS, checkIfIsToday);
	}

	private static int computeDaysFromPreviousExtractionDate(LocalDate startDate, List<DayOfWeek> extractionDates, boolean checkIfIsToday) {
		DayOfWeek currentDayOfWeek = startDate.getDayOfWeek();
		DayOfWeek previousExctractionDay = null;
		boolean isExtractionDate = false;
		for (int i = extractionDates.size() - 1; i >= 0; i--) {
			DayOfWeek extractionDay = extractionDates.get(i);
			if (currentDayOfWeek.compareTo(extractionDay) == 0) {
				isExtractionDate = true;
			}
			if (currentDayOfWeek.compareTo(extractionDay) > 0) {
				previousExctractionDay = extractionDay;
				break;
			}
		}
		if (previousExctractionDay != null) {
			return previousExctractionDay.ordinal() - currentDayOfWeek.ordinal();
		} else {
			if (checkIfIsToday) {
				LocalDateTime now = TimeUtils.now();
				if (now.toLocalDate().compareTo(startDate) == 0 && isExtractionDate && now.compareTo(toClosingTime(now)) > 0) {
					return 0;
				}
			}
			return (currentDayOfWeek.getValue() + (DayOfWeek.SUNDAY.ordinal() - extractionDates.get(extractionDates.size()-1).ordinal())) * -1;
		}
	}

	public static LocalDateTime toClosingTime(LocalDateTime time) {
		return time.withHour(19).withMinute(0).withSecond(0).withNano(0);
	}

	public static LocalDateTime toClosingTime(LocalDate date) {
		return TimeUtils.now().with(date).withHour(19).withMinute(0).withSecond(0).withNano(0);
	}

	public static LocalDateTime toAfterExtraction(LocalDate extractionDate) {
		return TimeUtils.now().with(extractionDate).withHour(21).withMinute(0).withSecond(0).withNano(0);
	}

}