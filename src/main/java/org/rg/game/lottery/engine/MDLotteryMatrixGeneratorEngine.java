package org.rg.game.lottery.engine;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.function.Function;
import java.util.function.Supplier;

import org.rg.game.core.TimeUtils;

public class MDLotteryMatrixGeneratorEngine extends LotteryMatrixGeneratorAbstEngine {
	private static final List<Map.Entry<Supplier<MDLotteryMatrixGeneratorEngine>, Properties>> allPreviousEngineAndConfigurations;

	static {
		allPreviousEngineAndConfigurations = new ArrayList<>();
	}

	public MDLotteryMatrixGeneratorEngine() {
		super();
	}

	@Override
	public LocalDate computeNextExtractionDate(LocalDate startDate, boolean incrementIfExpired) {
		LocalDateTime now = TimeUtils.now();
		if (incrementIfExpired) {
			while (now.compareTo(
				now.with(startDate).withHour(18).withMinute(45).withSecond(0).withNano(0)
			) > 0) {
				startDate = startDate.plus(1, ChronoUnit.DAYS);
			}
		}
		return startDate;
	}

	@Override
	protected int getIncrementDays(LocalDate startDate, boolean checkIfIsToday) {
		return 1;
	}

	@Override
	protected List<Entry<Supplier<MDLotteryMatrixGeneratorEngine>, Properties>> getAllPreviousEngineAndConfigurations() {
		return allPreviousEngineAndConfigurations;
	}

	@Override
	protected List<LocalDate> forWeekOf(LocalDate dayOfWeek) {
		List<LocalDate> dates = new ArrayList<>();
		LocalDate nextWeekStart = dayOfWeek.with(DayOfWeek.MONDAY);
		dates.add(nextWeekStart);
		for (int i = 0; i < 6; i++) {
			dates.add(nextWeekStart = nextWeekStart.plus(getIncrementDays(nextWeekStart, false), ChronoUnit.DAYS));
		}
		return dates;
	}

	@Override
	public Map<String, Object> adjustSeed(LocalDate extractionDate) {
		long seed = 1L;
		LocalDate seedStartDate = LocalDate.parse("2018-02-17");
		if (seedStartDate.compareTo(extractionDate) >= 0) {
			throw new IllegalArgumentException("Unvalid date: " + extractionDate);
		}
		while (seedStartDate.compareTo(extractionDate) < 0) {
			seedStartDate = seedStartDate.plus(getIncrementDays(seedStartDate, false), ChronoUnit.DAYS);
			seed++;
		}
		getProcessingContext().random = new Random(seed);
		buildComboIndexSupplier();
		Map<String, Object> seedData = new LinkedHashMap<>();
		seedData.put("seed", seed);
		seedData.put("seedStartDate", seedStartDate);
		return seedData;
	}

	@Override
	protected Function<String, Function<Integer, Function<Integer, Iterator<Integer>>>> getNumberGeneratorFactory(LocalDate extractionDate) {
		return generatorType-> leftBound -> rightBound -> {
			if (NumberProcessor.RANDOM_KEY.equals(generatorType)) {
				return getProcessingContext().random.ints(leftBound , rightBound + 1).iterator();
			}
			throw new IllegalArgumentException("Unvalid generator type");
		};
	}

	@Override
	protected Map<String, Object> testEffectiveness(String combinationFilterRaw, List<Integer> numbers, LocalDate extractionDate, boolean fineLog) {
		throw new UnsupportedOperationException("Effectiveness test");

	}

	@Override
	protected String getDefaultNumberRange() {
		return "1 -> 55";
	}

	@Override
	protected Map<String, Object> checkQuality(Storage storage, LocalDate extractionDate) {
		throw new UnsupportedOperationException("Check quality");
	}


	@Override
	public String getDefaultExtractionArchiveStartDate() {
		return "07/02/2018";
	}

	@Override
	public String getDefaultExtractionArchiveForSeedStartDate() {
		return "07/02/2018";
	}

	@Override
	protected Supplier<MDLotteryMatrixGeneratorEngine> newEngineBuilderWithId(int index) {
		return () -> {
			MDLotteryMatrixGeneratorEngine engine = new MDLotteryMatrixGeneratorEngine();
			engine.engineIndex = index;
			return engine;
		};
	}
}