package org.rg.game.lottery.application;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PrimitiveIterator.OfLong;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.burningwave.Throwables;
import org.rg.game.core.CollectionUtils;
import org.rg.game.core.ConcurrentUtils;
import org.rg.game.core.FirestoreWrapper;
import org.rg.game.core.IOUtils;
import org.rg.game.core.Info;
import org.rg.game.core.LogUtils;
import org.rg.game.core.MathUtils;
import org.rg.game.core.NetworkUtils;
import org.rg.game.core.ResourceUtils;
import org.rg.game.core.TimeUtils;
import org.rg.game.lottery.engine.ComboHandler;
import org.rg.game.lottery.engine.ComboHandler.IterationData;
import org.rg.game.lottery.engine.PersistentStorage;
import org.rg.game.lottery.engine.Premium;
import org.rg.game.lottery.engine.SELotteryMatrixGeneratorEngine;
import org.rg.game.lottery.engine.SEPremium;
import org.rg.game.lottery.engine.SEStats;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.ContextualDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NullNode;

public class SEIntegralSystemAnalyzer extends Shared {

	private static List<Function<String, Record>> recordLoaders;
	private static List<Function<String, Consumer<Record>>> recordWriters;
	private static List<Function<String, Consumer<Record>>> localRecordWriters;
	private static boolean timeoutReached;

	static {
		recordWriters = new ArrayList<>();
		recordLoaders = new ArrayList<>();
		localRecordWriters = new ArrayList<>();
		IOUtils.INSTANCE.getObjectMapper().registerModule(
			new SimpleModule().addDeserializer(Record.class, new Record.Deserializer())
		);
	}

	public static void main(String[] args) throws IOException {
		long startTime = System.currentTimeMillis();
		addFirebaseRecordLoaderAndWriter();
		addDefaultRecordLoader();
		addDefaultRecordWriter();
		addJSONRecordLoader();
		addJSONRecordWriter();

		String[] configurationFileFolders = ResourceUtils.INSTANCE.pathsFromSystemEnv(
			"working-path.integral-system-analysis.folder",
			"resources.integral-system-analysis.folder"
		);
		LogUtils.INSTANCE.info("Set configuration files folder to " + String.join(", ", configurationFileFolders));
		LogUtils.INSTANCE.info();
		List<File> configurationFiles =
			ResourceUtils.INSTANCE.find(
				"se-integral-systems-analysis", "properties",
				configurationFileFolders
			);
		int maxParallelTasks = CollectionUtils.INSTANCE.retrieveInteger(
			null,
			"tasks.max-parallel",
			Math.max((Runtime.getRuntime().availableProcessors() / 2) - 1, 1)
		);
		Collection<CompletableFuture<Void>> futures = new CopyOnWriteArrayList<>();
		boolean onlyShowComputed = CollectionUtils.INSTANCE.retrieveBoolean(null, "onlyShowComputed", false);
		if (onlyShowComputed) {
			LogUtils.INSTANCE.info("Analysis disabled");
		}
		Long timeout = CollectionUtils.INSTANCE.retrieveLong(null, "timeout");
		if (timeout != null) {
			LogUtils.INSTANCE.info("Set timeout to " + timeout + " seconds");
			Thread exiter = new Thread(() -> {
				long elapsedTimeFromStart = System.currentTimeMillis() - startTime;
				long effectiveTimeout = (timeout * 1000) - elapsedTimeFromStart;
				if (effectiveTimeout > 0) {
					try {
						Thread.sleep(effectiveTimeout);
					} catch (InterruptedException e) {

					}
				}
				LogUtils.INSTANCE.warn("Timeout reached");
				timeoutReached = true;
				//System.exit(0);
			});
			exiter.setDaemon(true);
			exiter.start();
		}
		Integer indexMode = CollectionUtils.INSTANCE.retrieveInteger(null, "index.mode");
		if (indexMode != null) {
			LogUtils.INSTANCE.info("Indexing mode: " + indexMode);
		}
		Integer indexModeFinal = indexMode;
		for (Properties config : ResourceUtils.INSTANCE.toOrderedProperties(configurationFiles)) {
			config.setProperty("numbers-processor.config.prefix", "choice-of-systems");
			String[] enabledRawValues = CollectionUtils.INSTANCE.retrieveValue(
				config, "enabled", "false"
			).split(";");
			boolean enabled = false;
			for (String enabledRawValue : enabledRawValues) {
				if (enabledRawValue.equals("true")) {
					enabled = true;
					break;
				} else if(enabledRawValue.toUpperCase().contains("onJDK".toUpperCase())) {
					if (Integer.valueOf(enabledRawValue.toUpperCase().replace("onJDK".toUpperCase(), "")).compareTo(Info.Provider.getInfoInstance().getVersion()) == 0) {
						enabled = true;
						break;
					}
				}
			}
			if (enabled) {
				Runnable task =
					onlyShowComputed ?
						() ->
							showComputed(config) :
					indexMode != null ?
						() ->
							index(config, indexModeFinal):
						() ->
							analyze(config);
				if (!onlyShowComputed && CollectionUtils.INSTANCE.retrieveBoolean(
						config,
						"async",
						false
					)
				) {
					ConcurrentUtils.INSTANCE.addTask(futures, task);
				} else {
					task.run();
				}
			}
			ConcurrentUtils.INSTANCE.waitUntil(futures, ft -> ft.size() >= maxParallelTasks);
		}
		futures.forEach(CompletableFuture::join);
		LogUtils.INSTANCE.warn("All activities are finished");
		FirestoreWrapper.shutdownDefaultInstance();
	}


	protected static void addFirebaseRecordLoaderAndWriter() throws FileNotFoundException, IOException {
		if (FirestoreWrapper.get() == null) {
			return;
		}
		addFirebaseRecordLoader();
		addFirebaseRecordWriter();
	}


	protected static void addFirebaseRecordWriter() {
		recordWriters.add(
			(String key) -> record -> {
				Map<String, Object> recordAsRawValue = new LinkedHashMap<>();
				recordAsRawValue.put("value", IOUtils.INSTANCE.writeToJSONFormat(record));
				FirestoreWrapper.get().write("IntegralSystemStats/"+key, recordAsRawValue);
			}
		);
	}


	protected static void addFirebaseRecordLoader() {
		recordLoaders.add(
			(String key) -> {
				return readFromJson((String)FirestoreWrapper.get().load("IntegralSystemStats/"+key).get("value"));
			}
		);
	}


	protected static void addDefaultRecordWriter() {
		String basePath = PersistentStorage.buildWorkingPath("Analisi sistemi integrali");
		Function<String, Consumer<Record>> writer = (String key) -> record -> {
			try {
				IOUtils.INSTANCE.store(basePath, key, record);
			} catch (Throwable exc) {
				//LogUtils.INSTANCE.error(exc, "Unable to store data to file system");
				//Throwables.INSTANCE.throwException(exc);
			}
		};
		recordWriters.add(writer);
		localRecordWriters.add(writer);
	}


	protected static void addJSONRecordWriter() {
		String basePath = PersistentStorage.buildWorkingPath("Analisi sistemi integrali");
		Function<String, Consumer<Record>> writer = (String key) -> record -> {
			try {
				IOUtils.INSTANCE.writeToJSONPrettyFormat(new File(basePath + "/" + key + ".json"), record);
			} catch (Throwable exc) {
				//LogUtils.INSTANCE.error(exc, "Unable to store data to file system");
				//Throwables.INSTANCE.throwException(exc);
			}
		};
		recordWriters.add(writer);
		localRecordWriters.add(writer);
	}


	protected static void addDefaultRecordLoader() {
		String basePath = PersistentStorage.buildWorkingPath("Analisi sistemi integrali");
		recordLoaders.add(
			(String key) -> {
				return
					IOUtils.INSTANCE.load(basePath, key);
			}
		);
	}

	protected static void addJSONRecordLoader() {
		String basePath = PersistentStorage.buildWorkingPath("Analisi sistemi integrali");
		recordLoaders.add(
			(String key) -> {
				return readFromJson(
					IOUtils.INSTANCE.fileToString(
							basePath + "/" + key + ".json",
							StandardCharsets.UTF_8
						)
					);
			}
		);
	}


	protected static void showComputed(Properties config) {
		ProcessingContext processingContext = new ProcessingContext(config);
		writeRecordToLocal(processingContext.cacheKey, processingContext.record);
		if (processingContext.record.data != null && !processingContext.record.data.isEmpty() &&
			processingContext.record.data.size() >= processingContext.rankSize
		) {
			//Sceglie una combinazione casuale fra quelle in classifica
			chooseAndPrintSelectedSystems(processingContext, config);
		}
		printData(processingContext.record, false);
		printBlocksInfo(processingContext);
	}


	protected static void index(Properties config, Integer indexMode) {
		ProcessingContext processingContext = new ProcessingContext(config);
		BigInteger processedBlock = BigInteger.ZERO;
		CompletableFuture<Void> writingTask = CompletableFuture.runAsync(() -> {});
		Collection<Block> toBeMerged = new CopyOnWriteArrayList<>();
		String cacheKey = indexMode.compareTo(0) >= 0 ?
			processingContext.cacheKey : buildCacheKey(
				processingContext.comboHandler, Shared.getSEStatsForLatestExtractionDate(), processingContext.premiumsToBeAnalyzed, processingContext.rankSize);
 		for (Block currentBlock : processingContext.record.blocks) {
			boolean writeRecord = false;
			processedBlock = processedBlock.add(BigInteger.ONE);
			synchronized(currentBlock) {
				if (currentBlock.indexes == null && indexMode.compareTo(0) > 0) {
					currentBlock.counter = currentBlock.start;
					currentBlock.indexes = processingContext.comboHandler.computeIndexes(currentBlock.start);
					List<Integer> combo = processingContext.comboHandler.toCombo(currentBlock.indexes);
					tryToAddCombo(
						processingContext,
						combo,
						computePremiums(processingContext, combo)
					);
					toBeMerged.add(currentBlock);
					writeRecord = processedBlock.mod(processingContext.modderForAutoSave).compareTo(BigInteger.ZERO) == 0 ||
						processedBlock.intValue() == processingContext.record.blocks.size();
				} else if (currentBlock.indexes != null && indexMode.compareTo(0) <= 0) {
					currentBlock.counter = null;
					currentBlock.indexes = null;
					writeRecord = processedBlock.mod(processingContext.modderForAutoSave).compareTo(BigInteger.ZERO) == 0 ||
						processedBlock.intValue() == processingContext.record.blocks.size();
				}
			}
			if (currentBlock.counter != null && currentBlock.counter.compareTo(currentBlock.start) < 0 && currentBlock.counter.compareTo(currentBlock.end) > 0) {
				LogUtils.INSTANCE.warn("Unaligned block: " + currentBlock);
			}
			if (writeRecord) {
				final BigInteger processedBlockOnStoring = new BigInteger(processedBlock.toString());
				writingTask.join();
				writingTask = CompletableFuture.runAsync(() -> {
					Block[] blocks = toBeMerged.stream().toArray(Block[]::new);
					merge(
						cacheKey,
						processingContext.record,
						processingContext.systemsRank,
						processingContext.rankSize
					);
					writeRecord(cacheKey, processingContext.record);
					for (Block block : blocks) {
						toBeMerged.remove(block);
					}
					LogUtils.INSTANCE.info(
						MathUtils.INSTANCE.format(processedBlockOnStoring) + " of " +
						MathUtils.INSTANCE.format(
							processingContext.record.blocks.size()) + " blocks have been " + (indexMode.compareTo(0) > 0 ? "indexed" : "unindexed"
						)
					);
				});
			}
		}
		writingTask.join();
		printData(processingContext.record, true);
	}

	protected static void analyze(Properties config) {
		ProcessingContext processingContext = new ProcessingContext(config);
		boolean printBlocks = CollectionUtils.INSTANCE.retrieveBoolean(config, "log.print.blocks", true);
		while (!processingContext.assignedBlocks.isEmpty() && !timeoutReached) {
			Iterator<Block> blockIterator = processingContext.assignedBlocks.iterator();
			while (blockIterator.hasNext() && !timeoutReached) {
				Block currentBlock = blockIterator.next();
				if (currentBlock.indexes == null) {
					currentBlock.indexes = processingContext.comboHandler.computeIndexes(currentBlock.start);
				}
				if (currentBlock.counter == null) {
					currentBlock.counter = processingContext.comboHandler.computeCounter(currentBlock.indexes);
				}
				processingContext.comboHandler.iterateFrom(
					processingContext.comboHandler.new IterationData(currentBlock.indexes, currentBlock.counter),
					iterationData -> {
						if (iterationData.getCounter().compareTo(currentBlock.end) > 0) {
							LogUtils.INSTANCE.warn("Right bound exceeded for " + currentBlock + ". Counter value: " + iterationData.getCounter());
							blockIterator.remove();
							iterationData.terminateIteration();
						}
						//Se altri runner remoti hanno modificato il blocco...
						if (currentBlock.counter.compareTo(iterationData.getCounter()) >= 0) {
							//... Allineiamo i blocchi
							mergeAndStore(
								processingContext.cacheKey,
								processingContext.record,
								processingContext.systemsRank,
								processingContext.rankSize
							);
							printDataIfChanged(
								processingContext.record,
								processingContext.previousLoggedRankWrapper,
								printBlocks
							);
							LogUtils.INSTANCE.info(
								"Skipping block " + currentBlock + " because it is being processed by others"
							);
							iterationData.terminateIteration();
						}
						currentBlock.counter = iterationData.getCounter();
						//Operazione spostata prima dell'operazione di store per motivi di performance:
						//in caso di anomalie decomentarla e cancellare la riga più in basso
						//assignedBlock.indexes = iterationData.copyOfIndexes();
						List<Integer> combo = iterationData.getCombo();
						Map<Number, Integer> allPremiums = computePremiums(processingContext, combo);
						if (filterCombo(allPremiums, Premium.TYPE_FIVE)) {
							tryToAddCombo(processingContext, combo, allPremiums);
						}
						if (iterationData.getCounter().mod(processingContext.modderForAutoSave).compareTo(BigInteger.ZERO) == 0 ||
							iterationData.getCounter().compareTo(currentBlock.end) == 0
							|| timeoutReached) {
							currentBlock.indexes = iterationData.copyOfIndexes(); //Ottimizzazione: in caso di anomalie eliminare questa riga e decommentare la riga più in alto (vedere commento)
							mergeAndStore(
								processingContext.cacheKey,
								processingContext.record,
								processingContext.systemsRank,
								processingContext.rankSize
							);
							printDataIfChanged(
								processingContext.record,
								processingContext.previousLoggedRankWrapper,
								printBlocks
							);
							printBlocksInfo(processingContext);
							if (timeoutReached) {
								iterationData.terminateIteration();
							}
			    		}
					}
				);
			}
			if (processingContext.assignedBlocks.isEmpty() && !timeoutReached) {
				processingContext.assignedBlocks.addAll(retrieveAssignedBlocks(config, processingContext.record));
			}
		}
		printData(processingContext.record, printBlocks);
		//LogUtils.INSTANCE.info(processedSystemsCounterWrapper.get() + " of combinations analyzed");
	}


	protected static boolean filterCombo(Map<Number, Integer> allPremiums, Integer premiumType) {
		boolean highWinningFound = false;
		for (Map.Entry<Number, Integer> premiumTypeAndCounter : allPremiums.entrySet()) {
			if (premiumTypeAndCounter.getKey().doubleValue() > premiumType.doubleValue() && premiumTypeAndCounter.getValue() > 0) {
				highWinningFound = true;
				break;
			}
		}
		return highWinningFound;
	}


	protected static boolean tryToAddCombo(ProcessingContext processingContext, List<Integer> combo,
			Map<Number, Integer> allPremiums) {
		Map.Entry<List<Integer>, Map<Number, Integer>> addedItem = new AbstractMap.SimpleEntry<>(combo, allPremiums);
		boolean addedItemFlag = processingContext.systemsRank.add(addedItem);
		if (processingContext.systemsRank.size() > processingContext.rankSize) {
			Map.Entry<List<Integer>, Map<Number, Integer>> removedItem = processingContext.systemsRank.pollLast();
			if (removedItem != addedItem) {
				//store(basePath, cacheKey, iterationData, systemsRank, cacheRecord, currentBlock, rankSize);
				LogUtils.INSTANCE.info(
					"Replaced data from rank:\n\t" + ComboHandler.toString(removedItem.getKey(), ", ") + ": " + removedItem.getValue() + "\n" +
					"\t\twith\n"+
					"\t" + ComboHandler.toString(addedItem.getKey(), ", ") + ": " + addedItem.getValue()
				);
			}
			return true;
		} else if (addedItemFlag) {
			//store(basePath, cacheKey, iterationData, systemsRank, cacheRecord, currentBlock, rankSize);
			LogUtils.INSTANCE.info("Added data to rank: " + ComboHandler.toString(combo, ", ") + ": " + allPremiums);
			return true;
		}
		return false;
	}


	protected static Map<Number, Integer> computePremiums(ProcessingContext processingContext, List<Integer> combo) {
		Map<Number, Integer> allPremiums = new LinkedHashMap<>();
		for (Number premiumType : processingContext.orderedPremiumsToBeAnalyzed) {
			allPremiums.put(premiumType, 0);
		}
		for (List<Integer> winningComboWithSuperStar : processingContext.allWinningCombosWithJollyAndSuperstar) {
			Map<Number, Integer> premiums = SEPremium.checkIntegral(combo, winningComboWithSuperStar);
			for (Map.Entry<Number, Integer> premiumTypeAndCounter : allPremiums.entrySet()) {
				Number premiumType = premiumTypeAndCounter.getKey();
				Integer premiumCounter = premiums.get(premiumType);
				if (premiumCounter != null) {
					allPremiums.put(premiumType, allPremiums.get(premiumType) + premiumCounter);
				}
			}
		}
		return allPremiums;
	}


	protected static Record readFromJson(String recordAsFlatRawValue) {
		if (recordAsFlatRawValue == null) {
			return null;
		}
		return IOUtils.INSTANCE.readFromJSONFormat(recordAsFlatRawValue, Record.class);
	}


	protected static void chooseAndPrintSelectedSystems(ProcessingContext processingContext, Properties config) {
		Record cacheRecord = processingContext.record;
		LocalDate nextExtractionDate = SELotteryMatrixGeneratorEngine.DEFAULT_INSTANCE.computeNextExtractionDate(LocalDate.now(), false);
		int rankSize = ProcessingContext.getRankSize(config);
		Map.Entry<LocalDate, Long> seedData = getSEAllStats().getSeedData(nextExtractionDate);
		Number premium = Premium.parseType(
			CollectionUtils.INSTANCE.retrieveValue(config, "choice-of-systems.filter", "0")
		);
		List<String> selectedCombosData = new ArrayList<>();
		Consumer<IterationData> premiumFilter = iterationData -> {
			List<Integer> cmb = iterationData.getCombo();
			Map<Number, Integer> premiums = computePremiums(processingContext, cmb);
			if (premium.intValue() == 0 || premiums.entrySet().stream().filter(
				(premiumTypeAndCounter) -> {
					return premiumTypeAndCounter.getKey().doubleValue() >= premium.doubleValue() &&
						premiumTypeAndCounter.getValue().compareTo(0) > 0;
				}
			).findFirst().orElseGet(() -> null) != null) {
				selectedCombosData.add(ComboHandler.toString(cmb) + ": " + Premium.toString(premiums, "=", ", "));
			}
		};
		if (CollectionUtils.INSTANCE.retrieveBoolean(config, "choice-of-systems.random", true)) {
			seedData.getValue();
			Long size = cacheRecord.blocks.stream().reduce((first, second) -> second)
			  .orElse(null).end.longValue();
			Random random = new Random(seedData.getValue());
			OfLong randomizer = random.longs(1L, size + 1).iterator();
			long nextLong = -1;
			while (nextLong > rankSize || nextLong < 0) {
				nextLong = randomizer.nextLong();
			}
			Map.Entry<List<Integer>, Map<Number, Integer>> combo = new ArrayList<>(cacheRecord.data).get(Long.valueOf(nextLong).intValue());
			ComboHandler cH = new ComboHandler(combo.getKey(), 6);
			cH.iterate(premiumFilter);
			StringBuffer log = new StringBuffer();
			log.append(
				"La combinazione scelta per il concorso " + seedData.getValue() + " del " +
				TimeUtils.defaultLocalDateFormat.format(nextExtractionDate) + " è:\n\t" + ComboHandler.toString(combo.getKey(), ", ") +
				"\nposizionata al " + nextLong + "° posto. Il relativo sistema è composto da " + selectedCombosData.size() + " combinazioni:"
			);
			for (String cmbData : selectedCombosData) {
				log.append("\t" + cmbData + "\n");
			}
			LogUtils.INSTANCE.info();
			LogUtils.INSTANCE.info(log.toString());
		}
		if (config.get("choice-of-systems.numbers") != null) {
			List<Integer> numbersToBePlayed =
				new SELotteryMatrixGeneratorEngine().setup(config, false).computeNumbersToBePlayed(nextExtractionDate);
			if (!numbersToBePlayed.isEmpty()) {
				TreeSet<Map.Entry<BigInteger, Integer>> rankComboPositionForNumbersPositions = new TreeSet<>((itemOne, itemTwo) -> {
					if (itemOne != itemTwo) {
						return itemOne.getValue().compareTo(itemTwo.getValue()) == 0 ?
							itemOne.getKey().compareTo(itemTwo.getKey()) :
								itemOne.getValue().compareTo(itemTwo.getValue()) > 0 ? -1 : 1;

					}
					return 0;
				});
				ComboHandler rankAsComboHandler = new ComboHandler(
					IntStream.range(0, rankSize).boxed().collect(Collectors.toList()),
					CollectionUtils.INSTANCE.retrieveInteger(config, "choice-of-systems.size")
				).iterate(iterationData -> {
					Set<Integer> allComboNumbers = new LinkedHashSet<>();
					for (Integer index : iterationData.getCombo()) {
						allComboNumbers.addAll(cacheRecord.data.get(index).getKey());
					}
					allComboNumbers.retainAll(numbersToBePlayed);
					rankComboPositionForNumbersPositions.add(new AbstractMap.SimpleEntry<BigInteger, Integer>(iterationData.getCounter(), allComboNumbers.size()));
					if (rankComboPositionForNumbersPositions.size() > 1) {
						rankComboPositionForNumbersPositions.pollLast();
					}
				});
				List<List<Integer>> selectedIntegralSystems = new ArrayList<>();
				for (Map.Entry<BigInteger, Integer> rankComboPositionForNumbersCounter : rankComboPositionForNumbersPositions) {
					List<Integer> selectedIntegralSystemsRow = new ArrayList<>();
					for (Integer selectedComboPosition : rankAsComboHandler.computeCombo(rankComboPositionForNumbersCounter.getKey())) {
						selectedIntegralSystemsRow.add(
							selectedComboPosition.intValue()
						);
					}
					selectedIntegralSystems.add(selectedIntegralSystemsRow);
				}
				StringBuffer log = new StringBuffer();
				log.append(
					"Le combinazioni scelte, sulla base dei numeri scelti (" +
					ComboHandler.toString(numbersToBePlayed, ", ") +
					") per il concorso " + seedData.getValue() + " del " +
					TimeUtils.defaultLocalDateFormat.format(nextExtractionDate) + " sono:\n"
				);
				Set<Integer> selectedIntegralSystemsIndexesFlat = new LinkedHashSet<>();
				List<List<Integer>> selectedIntegralSystemsFlat = new ArrayList<>();
				for (List<Integer> selectedIntegralSystemsRow : selectedIntegralSystems) {
					for (Integer systemRankPosition : selectedIntegralSystemsRow) {
						if (selectedIntegralSystemsIndexesFlat.add(systemRankPosition)) {
							List<Integer> selectedIntegralSystem = cacheRecord.data.get(systemRankPosition).getKey();
							selectedIntegralSystemsFlat.add(selectedIntegralSystem);
							log.append(
								"\t" + ComboHandler.toString(selectedIntegralSystem, ", ") +
								"\t posizionata al " + (systemRankPosition + 1) + "° posto.\n"
							);
						}
					}
				}
				for (List<Integer> selectedIntegralSystem : selectedIntegralSystemsFlat) {
					new ComboHandler(selectedIntegralSystem, 6).iterate(premiumFilter);
				}
				log.append("Il relativo sistema è composto da " + selectedCombosData.size() + " combinazioni:\n");
				for (String cmbData : selectedCombosData) {
					log.append("\t" + cmbData + "\n");
				}
				LogUtils.INSTANCE.info(log.toString());
			}
		}
	}


	protected static String buildCacheKey(ComboHandler comboHandler, SEStats sEStats, String premiumsToBeAnalyzed, int rankSize) {
		return "[" + MathUtils.INSTANCE.format(comboHandler.getSize()).replace(".", "_") + "][" + comboHandler.getCombinationSize() + "]" +
				"[" + premiumsToBeAnalyzed.replace(".", "_") + "]" + "[" + rankSize + "]" +
				"[" + TimeUtils.getAlternativeDateFormat().format(sEStats.getStartDate()) + "]" +
				"[" + TimeUtils.getAlternativeDateFormat().format(sEStats.getEndDate()) + "]";
	}

	protected static BigInteger processedBlockCounter(Record record) {
		BigInteger processed = BigInteger.ZERO;
		for (Block block : record.blocks) {
			if (block.counter != null && block.counter.compareTo(block.end) == 0) {
				processed = processed.add(BigInteger.ONE);
			}
		}
		return processed;
	}

	protected static BigInteger startedBlockCounter(Record record) {
		BigInteger started = BigInteger.ZERO;
		for (Block block : record.blocks) {
			if (block.counter != null && block.counter.compareTo(block.start) > 0 && block.counter.compareTo(block.end) < 0) {
				started = started.add(BigInteger.ONE);
			}
		}
		return started;
	}

	protected static BigInteger processedSystemsCounter(Record record) {
		BigInteger processed = BigInteger.ZERO;
		for (Block block : record.blocks) {
			if (block.counter != null) {
				processed = processed.add(block.counter.subtract(block.start.subtract(BigInteger.ONE)));
			}
		}
		return processed;
	}


	protected static BigInteger remainedSystemsCounter(Record record) {
		BigInteger processedSystemsCounter = processedSystemsCounter(record);
		Block latestBlock = CollectionUtils.INSTANCE.getLastElement(record.blocks);
		return latestBlock.end.subtract(processedSystemsCounter);
	}

	protected static BigInteger systemsCounter(Record record) {
		Block latestBlock = CollectionUtils.INSTANCE.getLastElement(record.blocks);
		return latestBlock.end;
	}


	protected static Record prepareCacheRecord(
		String cacheKey, ComboHandler cH,
		TreeSet<Map.Entry<List<Integer>, Map<Number, Integer>>> systemsRank
	){
		Record cacheRecordTemp = loadRecord(cacheKey);
		if (cacheRecordTemp != null) {
			systemsRank.addAll(cacheRecordTemp.data);
		} else {
			cacheRecordTemp = new Record();
		}
		if (cacheRecordTemp.blocks == null) {
			BigInteger blockSize = computeBlockSize(cH);
			cacheRecordTemp.blocks =
				divide(
					cH.getSize(),
					Math.min(
						cH.getSize().divide(blockSize).longValue(),
						new ComboHandler(SEStats.NUMBERS, 9L).getSize().divide(blockSize).longValue()
					)
				);
		}
		return cacheRecordTemp;
	}


	protected static BigInteger computeBlockSize(ComboHandler cH) {
		BigInteger blockSize =
			BigInteger.valueOf(
				Math.max(
					Math.min(
						cH.getSizeAsLong() / 50,
						100_000_000L
					),
				10_000_000L
			)
		);
		return blockSize;
	}


	protected static List<Block> retrieveAssignedBlocks(Properties config, Record cacheRecordTemp) {
		String blockAssignees = CollectionUtils.INSTANCE.retrieveValue(config, "blocks.assegnee");
		Collection<Block> blocks = new LinkedHashSet<>();
		boolean random = false;
		if (blockAssignees != null) {
			String thisHostName = NetworkUtils.INSTANCE.thisHostName();
			for (String blockAssignee : blockAssignees.replaceAll("\\s+","").split(";")) {
				String[] blockAssigneeInfo = blockAssignee.split(":");
				if (blockAssigneeInfo.length > 1 && blockAssigneeInfo[1].contains("random")) {
					random = true;
					blockAssigneeInfo[1] = blockAssigneeInfo[1].replace("random", "").replace("[", "").replace("]", "");
				}
				if (blockAssigneeInfo[0].equalsIgnoreCase(thisHostName) || blockAssigneeInfo[0].equals("all")) {
					if (blockAssigneeInfo[1].isEmpty() || blockAssigneeInfo[1].equals("all")) {
						blocks.addAll(cacheRecordTemp.blocks);
					} else {
						blocks.clear();
						for (String blockIndex : blockAssigneeInfo[1].split(",")) {
							if (blockIndex.equalsIgnoreCase("odd")) {
								blocks.addAll(CollectionUtils.INSTANCE.odd(cacheRecordTemp.blocks));
							} else if (blockIndex.equalsIgnoreCase("even")) {
								blocks.addAll(CollectionUtils.INSTANCE.even(cacheRecordTemp.blocks));
							} else if (blockIndex.contains("/")) {
								String[] subListsInfo = blockIndex.split("/");
								List<List<Block>> subList =
									CollectionUtils.INSTANCE.toSubLists((List<Block>)cacheRecordTemp.blocks,
										Double.valueOf(Math.ceil(((List<Block>)cacheRecordTemp.blocks).size() / Double.valueOf(subListsInfo[1]))).intValue()
									);
								blocks.addAll(subList.get(Integer.valueOf(subListsInfo[0]) - 1));
							} else {
								blocks.add(cacheRecordTemp.getBlock(Integer.valueOf(blockIndex) - 1));
							}
						}
						break;
					}
				} else if (blockAssigneeInfo[0].contains("random")) {
					blocks.addAll(cacheRecordTemp.blocks);
					random = true;
				}
			}
		} else {
			blocks.addAll(cacheRecordTemp.blocks);
		}
		Iterator<Block> blocksIterator = blocks.iterator();
		while (blocksIterator.hasNext()) {
			Block block = blocksIterator.next();
			BigInteger counter = block.counter;
			if (counter != null && counter.compareTo(block.end) == 0) {
				blocksIterator.remove();
			}
		}
		List<Block> toBeProcessed = new ArrayList<>(blocks);
		if (random) {
			Collections.shuffle(toBeProcessed) ;
		}
		return toBeProcessed;
	}


	protected static TreeSet<Map.Entry<List<Integer>, Map<Number, Integer>>> buildDataCollection(Number[] orderedPremiumsToBeAnalyzed) {
		TreeSet<Map.Entry<List<Integer>, Map<Number, Integer>>> bestSystems = new TreeSet<>((itemOne, itemTwo) -> {
			if (itemOne != itemTwo) {
				for(Number type : orderedPremiumsToBeAnalyzed) {
					int comparitionResult = itemOne.getValue().getOrDefault(type, 0).compareTo(itemTwo.getValue().getOrDefault(type, 0));
					if (comparitionResult != 0) {
						return comparitionResult * -1;
					}
				}
				for (int i = 0; i < Math.max(itemOne.getKey().size(), itemTwo.getKey().size()); i++) {
					int numberComparition = itemOne.getKey().get(i).compareTo(itemTwo.getKey().get(i));
					if (numberComparition != 0) {
						return numberComparition * -1;
					}
				}
			}
			return 0;
		});
		return bestSystems;
	}


	protected static void printData(
		Record record,
		boolean showBlockInfo
	) {
		String currentRank = String.join(
			"\n\t",
			record.data.stream().map(entry ->
				ComboHandler.toString(entry.getKey(), ", ") + ": " + Premium.toString(entry.getValue(), "=", ", ")
			).collect(Collectors.toList())
		);
		String currentLog = "";
		if (showBlockInfo) {
			currentLog += "\nBlocks (size: " + record.blocks.size() + ") status:\n" +
				"\t" + String.join(
					"\n\t",
					record.blocks.stream().map(Object::toString).collect(Collectors.toList())
				) + "\n";
		}
		currentLog += "Rank (size: " + record.data.size() + "):\n" +
			"\t" + currentRank + "\n";				;
		LogUtils.INSTANCE.info(currentLog);
	}


	protected static void printDataIfChanged(
		Record record,
		AtomicReference<String> previousLoggedRankWrapper,
		boolean showBlockInfo
	) {
		String currentRank = String.join(
			"\n\t",
			record.data.stream().map(entry ->
				ComboHandler.toString(entry.getKey(), ", ") + ": " + Premium.toString(entry.getValue(), "=", ", ")
			).collect(Collectors.toList())
		);
		String currentLog = "";
		if (showBlockInfo) {
			currentLog += "\nBlocks (size: " + record.blocks.size() + ") status:\n" +
				"\t" + String.join(
					"\n\t",
					record.blocks.stream().map(Object::toString).collect(Collectors.toList())
				) + "\n";
		}
		currentLog += "Rank (size: " + record.data.size() + "):\n" +
			"\t" + currentRank + "\n";
		String previousLoggedRank = previousLoggedRankWrapper.get();
		if (previousLoggedRank == null || !previousLoggedRank.equals(currentRank)) {
			LogUtils.INSTANCE.info(currentLog);
		}
		previousLoggedRankWrapper.set(currentRank);
	}


	protected static void printBlocksInfo(ProcessingContext processingContext) {
		BigInteger startedBlockCounter = startedBlockCounter(processingContext.record);
		LogUtils.INSTANCE.info(
			MathUtils.INSTANCE.format(processedSystemsCounter(processingContext.record)) + " of " +
			processingContext.sizeOfIntegralSystemMatrixAsString + " systems analyzed; " +
			MathUtils.INSTANCE.format(processedBlockCounter(processingContext.record)) + " blocks completed " +
			(startedBlockCounter.compareTo(BigInteger.ZERO) > 0 ?
				"and " + MathUtils.INSTANCE.format(startedBlockCounter) + " blocks started " :
				"") + "out of " +
			MathUtils.INSTANCE.format(processingContext.record.blocks.size()) + " total blocks"
		);
	}


	protected static Record loadRecord(String cacheKey) {
		List<Throwable> exceptions = new ArrayList<>();
		for (Function<String, Record> recordLoader : recordLoaders) {
			try {
				return recordLoader.apply(cacheKey);
			} catch (Throwable exc) {
				LogUtils.INSTANCE.error(exc, "Unable to load data: " + cacheKey);
				exceptions.add(exc);
				if (exceptions.size() == recordLoaders.size()) {
					return Throwables.INSTANCE.throwException(exceptions.get(0));
				}
			}
		}
		return null;
	}


	protected static void writeRecord(String cacheKey, Record toBeCached) {
		writeRecordTo(cacheKey, toBeCached, recordWriters);
	}

	protected static void writeRecordToLocal(String cacheKey, Record toBeCached) {
		writeRecordTo(cacheKey, toBeCached, localRecordWriters);
	}


	protected static void writeRecordTo(
		String cacheKey,
		Record toBeCached,
		List<Function<String,
		Consumer<Record>>> recordWriters
	) {
		List<Throwable> exceptions = new ArrayList<>();
		for (Function<String, Consumer<Record>> recordWriter : recordWriters) {
			try {
				recordWriter.apply(cacheKey).accept(toBeCached);
			} catch (Throwable exc) {
				LogUtils.INSTANCE.error(exc, "Unable to store data");
				exceptions.add(exc);
				if (exceptions.size() == recordLoaders.size()) {
					Throwables.INSTANCE.throwException(exceptions.get(0));
				}
			}
		}
	}


	private static void mergeAndStore(
		String cacheKey,
		Record toBeCached,
		TreeSet<Entry<List<Integer>, Map<Number, Integer>>> systemsRank,
		int rankSize
	){
		merge(cacheKey, toBeCached, systemsRank, rankSize);
		writeRecord(cacheKey, toBeCached);
	}


	private static void merge(
		String cacheKey,
		Record toBeCached,
		TreeSet<Entry<List<Integer>, Map<Number, Integer>>> systemsRank,
		int rankSize
	){
		Record cachedRecord = loadRecord(cacheKey);
		//long elapsedTime = System.currentTimeMillis();
		if (cachedRecord != null) {
			systemsRank.addAll(cachedRecord.data);
			List<Block> cachedBlocks = (List<Block>)cachedRecord.blocks;
			List<Block> toBeCachedBlocks = (List<Block>)toBeCached.blocks;
			for (int i = 0; i < toBeCachedBlocks.size(); i++) {
				Block toBeCachedBlock = toBeCachedBlocks.get(i);
				Block cachedBlock = cachedBlocks.get(i);
				BigInteger cachedBlockCounter = cachedBlock.counter;
				if (cachedBlockCounter != null && (toBeCachedBlock.counter == null || cachedBlockCounter.compareTo(toBeCachedBlock.counter) > 0)) {
					toBeCachedBlock.counter = cachedBlock.counter;
					toBeCachedBlock.indexes = cachedBlock.indexes;
				}
			}
		}
		while (systemsRank.size() > rankSize) {
			systemsRank.pollLast();
		}
		toBeCached.data = new ArrayList<>(systemsRank);
		//elapsedTime = System.currentTimeMillis() - elapsedTime;
		//LogUtils.INSTANCE.info("milliseconds " + elapsedTime);
	}


	protected static void store(
		String basePath,
		String cacheKey,
		Record record,
		Record cacheRecord
	) {
		cacheRecord.data = new ArrayList<>(record.data);
		IOUtils.INSTANCE.store(basePath, cacheKey, cacheRecord);
	}


	public static List<Block> divide(BigInteger size, long blockNumber) {
		BigInteger blockSize = size.divide(BigInteger.valueOf(blockNumber));
		BigInteger remainedSize = size.mod(BigInteger.valueOf(blockNumber));
		List<Block> blocks = new ArrayList<>();
		BigInteger blockStart = BigInteger.ONE;
		for (int i = 0; i < blockNumber; i++) {
			BigInteger blockEnd = blockStart.add(blockSize.subtract(BigInteger.ONE));
			blocks.add(new Block(blockStart, blockEnd, null, null));
			blockStart = blockEnd.add(BigInteger.ONE);
		}
		if (remainedSize.compareTo(BigInteger.ZERO) != 0) {
			blocks.add(new Block(blockStart, blockStart.add(remainedSize.subtract(BigInteger.ONE)), null, null));
		}
		return blocks;
	}


	public static class Record implements Serializable {

		private static final long serialVersionUID = -5223969149097163659L;

		Record() {}

		Record(List<Block> blocks, List<Map.Entry<List<Integer>, Map<Number, Integer>>> data) {
			this.blocks = blocks;
			this.data = data;
		}

		@JsonProperty("blocks")
		private List<Block> blocks;

		@JsonProperty("data")
		private List<Map.Entry<List<Integer>, Map<Number, Integer>>> data;

		public Block getBlock(int index) {
			return ((List<Block>)blocks).get(index);
		}

		public static class Deserializer extends JsonDeserializer<Record> implements ContextualDeserializer {

			@Override
			public Record deserialize(
				JsonParser jsonParser, DeserializationContext context
			) throws IOException, JacksonException {
				JsonNode recordNode = jsonParser.getCodec().readTree(jsonParser);
				Iterator<JsonNode> blockNodeIterator = recordNode.get("blocks").iterator();
				List<Block> blocks = new ArrayList<>();
				while (blockNodeIterator.hasNext()) {
					JsonNode blockNode = blockNodeIterator.next();
					JsonNode indexesNode = blockNode.get("indexes");
					int[] indexes = null;
					if (indexesNode instanceof ArrayNode) {
						Iterator<JsonNode> indexesNodeIterator = indexesNode.iterator();
						indexes = new int[indexesNode.size()];
						for (int i = 0; indexesNodeIterator.hasNext(); i++) {
							indexes[i] = indexesNodeIterator.next().asInt();
						}
					}
					JsonNode counterNode = blockNode.get("counter");
					BigInteger counter = !(counterNode instanceof NullNode)? new BigInteger(counterNode.asText()) : null;
					blocks.add(
						new Block(
							new BigInteger(blockNode.get("start").asText()),
							new BigInteger(blockNode.get("end").asText()),
							counter,
							indexes
						)
					);
				}

				List<Map.Entry<List<Integer>, Map<Number, Integer>>> data = new ArrayList<>();
				recordNode.get("data").forEach(comboForResultNode -> {
					Map<Number, Integer> premiums = new LinkedHashMap<>();
					comboForResultNode.forEach(premiumsNode -> {
						for (String premiumForCounterRaw : premiumsNode.toString().replaceAll("\"|\\{|\\}| ", "").split(",")) {
							String[] premiumForCounter = premiumForCounterRaw.split(":");
							premiums.put(
								Premium.parseType(premiumForCounter[0]), Integer.valueOf(premiumForCounter[1])
							);
						}
					});
					data.add(
						new AbstractMap.SimpleEntry<>(
							ComboHandler.fromString(comboForResultNode.toString().split(":")[0].replaceAll("\"|\\{|\\[|\\]", "")),
							premiums
						)
					);
				});

				return new Record(blocks, data);
			}


			@Override
			public JsonDeserializer<?> createContextual(
				DeserializationContext ctxt,
				BeanProperty property
			) throws JsonMappingException {
		        return this;
			}
		}

	}

	public static class Block implements Serializable {

		private static final long serialVersionUID = 1725710713018555234L;

		@JsonProperty("start")
		private BigInteger start;

		@JsonProperty("end")
		private BigInteger end;

		@JsonProperty("counter")
		private BigInteger counter;

		@JsonProperty("indexes")
		private int[] indexes;

		public Block(BigInteger start, BigInteger end, BigInteger counter, int[] indexes) {
			this.start = start;
			this.end = end;
			this.counter = counter;
			this.indexes = indexes;
		}

		@Override
		public String toString() {
			return "Block [start=" + start + ", end=" + end + ", counter=" + counter + ", indexes="
					+ Arrays.toString(indexes) + "]";
		}


	}

	private static class ProcessingContext {
		private List<Block> assignedBlocks;
		private Record record;
		private Integer rankSize;
		private ComboHandler comboHandler;
		private BigInteger modderForSkipLog;
		private AtomicReference<String> previousLoggedRankWrapper;
		private Number[] orderedPremiumsToBeAnalyzed;
		private Collection<List<Integer>> allWinningCombosWithJollyAndSuperstar;
		private TreeSet<Map.Entry<List<Integer>, Map<Number, Integer>>> systemsRank;
		private BigInteger modderForAutoSave;
		private String cacheKey;
		private String premiumsToBeAnalyzed;
		private BigInteger sizeOfIntegralSystemMatrix;
		private String sizeOfIntegralSystemMatrixAsString;

		private ProcessingContext(Properties config) {
			premiumsToBeAnalyzed = CollectionUtils.INSTANCE.retrieveValue(
				config,
				"rank.premiums",
				String.join(",", Premium.allTypesListReversed().stream().map(Object::toString).collect(Collectors.toList()))
			).replaceAll("\\s+","");
			orderedPremiumsToBeAnalyzed =
				Arrays.asList(
					premiumsToBeAnalyzed.split(",")
				).stream().map(Premium::parseType).toArray(Number[]::new);
			long combinationSize = CollectionUtils.INSTANCE.retrieveLong(
				config,
				"combination.components"
			);
			comboHandler = new ComboHandler(SEStats.NUMBERS, combinationSize);
			modderForSkipLog = BigInteger.valueOf(1_000_000_000);
			modderForAutoSave = new BigInteger(
				CollectionUtils.INSTANCE.retrieveValue(
					config,
					"autosave-every",
					"1000000"
				)
			);
			rankSize = getRankSize(config);
			SEStats sEStats = SEStats.get(
				CollectionUtils.INSTANCE.retrieveValue(config, "competition.archive.start-date"),
				CollectionUtils.INSTANCE.retrieveValue(config, "competition.archive.end-date")
			);
			allWinningCombosWithJollyAndSuperstar = sEStats.getAllWinningCombosWithJollyAndSuperstar().values();
			LogUtils.INSTANCE.info("All " + combinationSize + " based integral systems size (" + comboHandler.getNumbers().size() + " numbers): " +  MathUtils.INSTANCE.format(comboHandler.getSize()));
			cacheKey = buildCacheKey(comboHandler, sEStats, premiumsToBeAnalyzed, rankSize);
			systemsRank = buildDataCollection(orderedPremiumsToBeAnalyzed);
			record = prepareCacheRecord(
				cacheKey,
				comboHandler,
				systemsRank
			);
			assignedBlocks = retrieveAssignedBlocks(config, record);
			previousLoggedRankWrapper = new AtomicReference<>();
			sizeOfIntegralSystemMatrix = comboHandler.getSize();
			sizeOfIntegralSystemMatrixAsString = MathUtils.INSTANCE.format(sizeOfIntegralSystemMatrix);
		}

		protected static Integer getRankSize(Properties config) {
			return CollectionUtils.INSTANCE.retrieveInteger(config, "rank.size", 100);
		}
	}

}
