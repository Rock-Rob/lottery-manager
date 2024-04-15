package org.rg.game.lottery.application;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.zip.ZipException;

import org.apache.poi.EmptyFileException;
import org.apache.poi.common.usermodel.HyperlinkType;
import org.apache.poi.ooxml.POIXMLException;
import org.apache.poi.openxml4j.exceptions.NotOfficeXmlFileException;
import org.apache.poi.openxml4j.exceptions.PartAlreadyExistsException;
import org.apache.poi.ss.SpreadsheetVersion;
import org.apache.poi.ss.formula.BaseFormulaEvaluator;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.ComparisonOperator;
import org.apache.poi.ss.usermodel.DataFormat;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.ss.usermodel.Hyperlink;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.util.CellReference;
import org.apache.poi.util.RecordFormatException;
import org.apache.poi.xssf.usermodel.XSSFFont;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.xmlbeans.impl.values.XmlValueDisconnectedException;
import org.burningwave.Synchronizer;
import org.burningwave.Synchronizer.Mutex;
import org.burningwave.Throwables;
import org.burningwave.ThrowingConsumer;
import org.rg.game.core.CollectionUtils;
import org.rg.game.core.ConcurrentUtils;
import org.rg.game.core.FirestoreWrapper;
import org.rg.game.core.IOUtils;
import org.rg.game.core.LogUtils;
import org.rg.game.core.NetworkUtils;
import org.rg.game.core.ResourceUtils;
import org.rg.game.core.TimeUtils;
import org.rg.game.lottery.engine.PersistentStorage;
import org.rg.game.lottery.engine.Premium;
import org.rg.game.lottery.engine.SELotteryMatrixGeneratorEngine;
import org.rg.game.lottery.engine.SEStats;
import org.rg.game.lottery.engine.SimpleWorkbookTemplate;
import org.rg.game.lottery.engine.Storage;

import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;

public class SELotterySimpleSimulator extends Shared {

	static final String DATA_FOLDER_NAME = "data";

	static final String RESULTS_LABEL = "Risultati";
	static final String BALANCE_LABEL = "Saldo";
	static final String RETURN_LABEL = "Ritorno";
	static final String COST_LABEL = "Costo";
	static final String EXTRACTION_DATE_LABEL = "Data";
	static final String HISTORICAL_LABEL = "(storico)";
	static final String HISTORICAL_BALANCE_LABEL = String.join(" ", Arrays.asList(BALANCE_LABEL, HISTORICAL_LABEL));
	static final String HISTORICAL_RETURN_LABEL = String.join(" ", Arrays.asList(RETURN_LABEL, HISTORICAL_LABEL));
	static final String HISTORICAL_COST_LABEL = String.join(" ", Arrays.asList(COST_LABEL, HISTORICAL_LABEL));
	static final String FOLLOWING_PROGRESSIVE_HISTORICAL_LABEL = "(st. prg. ant.)";
	static final String FOLLOWING_PROGRESSIVE_HISTORICAL_BALANCE_LABEL = String.join(" ", Arrays.asList(BALANCE_LABEL, FOLLOWING_PROGRESSIVE_HISTORICAL_LABEL));
	static final String FOLLOWING_PROGRESSIVE_HISTORICAL_RETURN_LABEL = String.join(" ", Arrays.asList(RETURN_LABEL, FOLLOWING_PROGRESSIVE_HISTORICAL_LABEL));
	static final String FOLLOWING_PROGRESSIVE_HISTORICAL_COST_LABEL = String.join(" ", Arrays.asList(COST_LABEL, FOLLOWING_PROGRESSIVE_HISTORICAL_LABEL));
	static final String FILE_LABEL = "File";
	static final String HISTORICAL_UPDATE_DATE_LABEL = "Data agg. storico";

	static final int[] COLUMN_SIZE = {
		3000,
		3800,
		24000
	};

	static final List<String> reportHeaderLabels;
	static final List<String> summaryFormulas;
	static final Map<String, Integer> cellIndexesCache;
	static final Map<Integer, String> indexToLetterCache;
	static final Map<String, Integer> savingOperationCounters;
	static final Pattern regexForExtractConfigFileName;
	static final String hostName;
	static final SEStats allTimeStats;
	static final List<List<String>> header;

	public static void main(String[] args) throws IOException {
		Collection<CompletableFuture<Void>> futures = new CopyOnWriteArrayList<>();
		executeRecursive(SELotterySimpleSimulator::execute, futures);
		LogUtils.INSTANCE.warn("All activities are finished");
		FirestoreWrapper.shutdownDefaultInstance();
	}


	static final Comparator<Row> rowsForDateComparator = (rowOne, rowTwo) -> {
		return rowOne.getCell(getCellIndex(rowOne.getSheet(), EXTRACTION_DATE_LABEL)).getDateCellValue().compareTo(
			rowTwo.getCell(getCellIndex(rowTwo.getSheet(), EXTRACTION_DATE_LABEL)).getDateCellValue()
		);
	};

	static {
		cellIndexesCache = new ConcurrentHashMap<>();
		indexToLetterCache = new ConcurrentHashMap<>();
		savingOperationCounters = new ConcurrentHashMap<>();
		hostName = NetworkUtils.INSTANCE.thisHostName();
		regexForExtractConfigFileName = Pattern.compile("\\[.*?\\]\\[.*?\\]\\[.*?\\](.*)\\.txt");
		allTimeStats = SEStats.get(SEStats.FIRST_EXTRACTION_DATE_AS_STRING, TimeUtils.getDefaultDateFormat().format(new Date()));
		SEStats.get(SEStats.FIRST_EXTRACTION_DATE_WITH_NEW_MACHINE_AS_STRING, TimeUtils.getDefaultDateFormat().format(new Date()));

		reportHeaderLabels = new ArrayList<>();
		reportHeaderLabels.add(EXTRACTION_DATE_LABEL);
		Collection<String> allPremiumLabels = Premium.allLabelsList();
		reportHeaderLabels.addAll(allPremiumLabels);
		reportHeaderLabels.add(COST_LABEL);
		reportHeaderLabels.add(RETURN_LABEL);
		reportHeaderLabels.add(BALANCE_LABEL);
		List<String> historyLabels = allPremiumLabels.stream().map(label -> getFollowingProgressiveHistoricalPremiumLabel(label)).collect(Collectors.toList());
		reportHeaderLabels.addAll(historyLabels);
		reportHeaderLabels.add(FOLLOWING_PROGRESSIVE_HISTORICAL_COST_LABEL);
		reportHeaderLabels.add(FOLLOWING_PROGRESSIVE_HISTORICAL_RETURN_LABEL);
		reportHeaderLabels.add(FOLLOWING_PROGRESSIVE_HISTORICAL_BALANCE_LABEL);
		historyLabels = allPremiumLabels.stream().map(label -> getHistoryPremiumLabel(label)).collect(Collectors.toList());
		reportHeaderLabels.addAll(historyLabels);
		reportHeaderLabels.add(HISTORICAL_COST_LABEL);
		reportHeaderLabels.add(HISTORICAL_RETURN_LABEL);
		reportHeaderLabels.add(HISTORICAL_BALANCE_LABEL);
		reportHeaderLabels.add(HISTORICAL_UPDATE_DATE_LABEL);
		reportHeaderLabels.add(FILE_LABEL);
		summaryFormulas = new ArrayList<>();

		header = Arrays.asList(
			reportHeaderLabels,
			summaryFormulas
		);
		String columnName = convertNumToColString(0);
		summaryFormulas.add("FORMULA_COUNTA(" + columnName + (getHeaderSize() + 1) + ":"+ columnName + getMaxRowIndexInExcelFormat() +")");
		for (int i = 1; i < reportHeaderLabels.size()-2; i++) {
			columnName = convertNumToColString(i);
			summaryFormulas.add(
				"FORMULA_SUM(" + columnName + (getHeaderSize() + 1) + ":"+ columnName + getMaxRowIndexInExcelFormat() +")"
			);
		}
		summaryFormulas.add("");
		summaryFormulas.add("");
	}

	protected static void executeRecursive(
		BiFunction<
			String,
			Collection<CompletableFuture<Void>>,
			Collection<CompletableFuture<Void>>
		> executor,
		Collection<CompletableFuture<Void>> futures
	) {
		try {
			executor.apply("se", futures).stream().forEach(CompletableFuture::join);
		} catch (Throwable exc) {
			LogUtils.INSTANCE.error(exc);
			executeRecursive(executor, futures);
		}
	}

	protected static Collection<CompletableFuture<Void>> execute(
		String configFilePrefix,
		Collection<CompletableFuture<Void>> futures
	) {
		Supplier<SELotteryMatrixGeneratorEngine> engineSupplier = SELotteryMatrixGeneratorEngine::new;
		String[] configurationFileFolders = ResourceUtils.INSTANCE.pathsFromSystemEnv(
			"working-path.simulations.folder",
			"resources.simulations.folder"
		);
		LogUtils.INSTANCE.info("Set configuration files folder to " + String.join(", ", configurationFileFolders) + "\n");
		List<File> configurationFiles =
			ResourceUtils.INSTANCE.find(
				configFilePrefix + "-simple-simulation", "properties",
				configurationFileFolders
			);
		try {
			prepareAndProcess(
				futures,
				engineSupplier,
				toConfigurations(
					configurationFiles,
					"simulation.slave"
				)
			);
		} catch (IOException exc) {
			Throwables.INSTANCE.throwException(exc);
		}
		return futures;
	}

	protected static List<Properties> toConfigurations(List<File> configurationFiles, String key) throws IOException {
		String forceSlave = System.getenv("forceSlave");
		String forceMaster = System.getenv("forceMaster");
		List<Properties> configurations = ResourceUtils.INSTANCE.toOrderedProperties(configurationFiles);
		if (forceMaster != null && Boolean.valueOf(forceMaster.replaceAll("\\s+",""))) {
			configurations.stream().forEach(config -> config.setProperty(key, "false"));
		}
		if (forceSlave != null && Boolean.valueOf(forceSlave.replaceAll("\\s+",""))) {
			configurations.stream().forEach(config -> config.setProperty(key, "true"));
		}
		return configurations;
	}

	private static Integer getOrPutAndGetCellIndex(Sheet sheet, String label) {
		return cellIndexesCache.computeIfAbsent(sheet.getSheetName() + "." + label, lb -> getCellIndex(sheet, label));
	}

	private static String convertNumToColString(Integer idx) {
		return indexToLetterCache.computeIfAbsent(idx, index -> CellReference.convertNumToColString(index));
	}

	protected static void prepareAndProcess(
		Collection<CompletableFuture<Void>> futures,
		Supplier<SELotteryMatrixGeneratorEngine> engineSupplier,
		List<Properties> configurationProperties
	) throws IOException {
		List<Properties> configurations = new ArrayList<>();
		List<String> configurationFileNames = new ArrayList<>();
		Set<String> groupsToBeProcessed = new TreeSet<>();
		boolean isFirst = true;
		for (Properties config : configurationProperties) {
			String simulationDates = CollectionUtils.INSTANCE.retrieveValue(config, "simulation.dates");
			if (simulationDates != null) {
				config.setProperty("competition", simulationDates);
			}
			String group = setGroup(config);
			config.setProperty("storage", "filesystem");
			config.setProperty("overwrite-if-exists", String.valueOf(CollectionUtils.INSTANCE.retrieveBoolean(config, "simulation.slave", false)? -1 : 0));
			if (CollectionUtils.INSTANCE.retrieveBoolean(config, "simulation.enabled", false)) {
				configurations.add(config);
				groupsToBeProcessed.add(group);
				configurationFileNames.add(CollectionUtils.INSTANCE.retrieveValue(config, "file.name"));
			}
		}
		LogUtils.INSTANCE.info(
			"Total files that will be processed: " + configurationFileNames.size() + "\n\n" +
			"\t" + String.join("\n\t", configurationFileNames) + "\n"
		);
		LogUtils.INSTANCE.info(
			"Total groups that will be processed: " + groupsToBeProcessed.size() + "\n\n" +
			"\t" + String.join("\n\t", groupsToBeProcessed) + "\n"
		);
		int maxParallelTasks = Optional.ofNullable(
			System.getenv("tasks.max-parallel")
		).map(Integer::valueOf).orElseGet(() -> Math.max((Runtime.getRuntime().availableProcessors() / 2) - 1, 1));
		for (Properties config : configurations) {
			LogUtils.INSTANCE.info(
				"Processing file '" + CollectionUtils.INSTANCE.retrieveValue(config, "file.name") + "' located in '" + CollectionUtils.INSTANCE.retrieveValue(config, "file.parent.absolutePath") + "' in " +
					(CollectionUtils.INSTANCE.retrieveBoolean(config, "simulation.slave") ? "slave" : "master") + " mode"
			);
			String info = CollectionUtils.INSTANCE.retrieveValue(config, "info");
			if (info != null) {
				LogUtils.INSTANCE.info(info);
			}
			String excelFileName = retrieveExcelFileName(config);
			String configFileName = CollectionUtils.INSTANCE.retrieveValue(config, "file.name").replace("." + CollectionUtils.INSTANCE.retrieveValue(config, "file.extension"), "");
			config.setProperty(
				"nameSuffix",
				configFileName
			);
			Collection<LocalDate> competitionDatesFlat = SELotteryMatrixGeneratorEngine.DEFAULT_INSTANCE.computeExtractionDates(CollectionUtils.INSTANCE.retrieveValue(config, "competition"));
			String redundantConfigValue = CollectionUtils.INSTANCE.retrieveValue(config, "simulation.redundancy");
			cleanup(
				config,
				excelFileName,
				competitionDatesFlat,
				configFileName,
				redundantConfigValue != null? Integer.valueOf(redundantConfigValue) : null
			);
			List<List<LocalDate>> competitionDates =
				CollectionUtils.INSTANCE.toSubLists(
					new ArrayList<>(competitionDatesFlat),
					redundantConfigValue != null? Integer.valueOf(redundantConfigValue) : 10
				);
			config.setProperty("report.enabled", "true");
			AtomicBoolean firstSetupExecuted = new AtomicBoolean(false);
			Runnable taskOperation = () -> {
				LogUtils.INSTANCE.info("Computation of " + CollectionUtils.INSTANCE.retrieveValue(config, "file.name") + " started");
				process(config, excelFileName, engineSupplier.get(), competitionDates, firstSetupExecuted);
				LogUtils.INSTANCE.info("Computation of " + CollectionUtils.INSTANCE.retrieveValue(config, "file.name") + " succesfully finished");
			};
			String asyncFlag = CollectionUtils.INSTANCE.retrieveValue(config, "async", "false");
			boolean async = false;
			if (asyncFlag.equalsIgnoreCase("onSlave")) {
				async = CollectionUtils.INSTANCE.retrieveBoolean(config, "simulation.slave", false);
			} else {
				async = CollectionUtils.INSTANCE.retrieveBoolean(config, "async", false);
			}
			if (async) {
				ConcurrentUtils.INSTANCE.addTask(futures, taskOperation);
			} else {
				taskOperation.run();
			}
			ConcurrentUtils.INSTANCE.waitUntil(futures, ft -> ft.size() >= maxParallelTasks);
			if (!firstSetupExecuted.get()) {
				synchronized (firstSetupExecuted) {
					if (!firstSetupExecuted.get()) {
						try {
							firstSetupExecuted.wait();
						} catch (InterruptedException exc) {
							Throwables.INSTANCE.throwException(exc);
						}
					}
				}
			}
		}
	}

	protected static String retrieveExcelFileName(Properties config) {
		String groupName = CollectionUtils.INSTANCE.retrieveValue(config, "simulation.group");
		PersistentStorage.buildWorkingPath(groupName);
		String reportFileName = (groupName.contains("\\") ?
			groupName.substring(groupName.lastIndexOf("\\") + 1) :
				groupName.contains("/") ?
				groupName.substring(groupName.lastIndexOf("/") + 1) :
					groupName) + "-report.xlsx";
		return groupName + File.separator + reportFileName;
	}

	protected static String setGroup(Properties config) {
		String simulationGroup = CollectionUtils.INSTANCE.retrieveValue(config, "simulation.group");
		if (simulationGroup == null) {
			simulationGroup = CollectionUtils.INSTANCE.retrieveValue(config, "file.name").replace("." + CollectionUtils.INSTANCE.retrieveValue(config, "file.extension"), "");
		}
		simulationGroup = simulationGroup.replace("${localhost.name}", hostName);
		config.setProperty("simulation.group", simulationGroup);
		config.setProperty(
			"group",
			simulationGroup + "/" + DATA_FOLDER_NAME
		);
		return simulationGroup;
	}


	protected static String retrieveDataBasePath(Properties config) {
		return  PersistentStorage.buildWorkingPath(CollectionUtils.INSTANCE.retrieveValue(config, "group"));
	}

	protected static LocalDate removeNextOfLatestExtractionDate(Properties config, Collection<LocalDate> extractionDates) {
		Iterator<LocalDate> extractionDatesIterator = extractionDates.iterator();
		LocalDate latestExtractionArchiveStartDate = TimeUtils.toLocalDate(getSEStats(config).getLatestExtractionDate());
		LocalDate nextAfterLatest = null;
		while (extractionDatesIterator.hasNext()) {
			LocalDate currentIterated = extractionDatesIterator.next();
			if (currentIterated.compareTo(latestExtractionArchiveStartDate) > 0) {
				if (nextAfterLatest == null) {
					nextAfterLatest = currentIterated;
				}
				extractionDatesIterator.remove();
			}
		}
		return nextAfterLatest;
	}

	protected static void cleanup(
		Properties config,
		String excelFileName,
		Collection<LocalDate> competitionDates,
		String configFileName, Integer redundancy
	) {
		removeNextOfLatestExtractionDate(config, competitionDates);
		int initialSize = competitionDates.size();
		if (redundancy != null) {
			cleanupRedundant(
				config,
				excelFileName, configFileName, redundancy, competitionDates
			);
		}
		boolean isSlave = CollectionUtils.INSTANCE.retrieveBoolean(config, "simulation.slave", false);
		readOrCreateExcel(
			excelFileName,
			workBook -> {
				Iterator<Row> rowIterator = workBook.getSheet(RESULTS_LABEL).rowIterator();
				rowIterator.next();
				rowIterator.next();
				while (rowIterator.hasNext()) {
					Row row = rowIterator.next();
					Cell date = row.getCell(0);
					if (rowRefersTo(row, configFileName)) {
						Iterator<LocalDate> competitionDatesItr = competitionDates.iterator();
						while (competitionDatesItr.hasNext()) {
							LocalDate competitionDate = competitionDatesItr.next();
							if (date != null && competitionDate.compareTo(TimeUtils.toLocalDate(date.getDateCellValue())) == 0) {
								competitionDatesItr.remove();
							}
						}
					}
				}
			},
			workBook ->
				createWorkbook(workBook, excelFileName),
			workBook -> {
				if (!isSlave) {
					store(excelFileName, workBook);
				}
			},
			isSlave
		);
		LogUtils.INSTANCE.info(competitionDates.size() + " dates will be processed, " + (initialSize - competitionDates.size()) + " already processed for file " + CollectionUtils.INSTANCE.retrieveValue(config, "file.name"));
	}

	private static void cleanupRedundant(Properties config, String excelFileName, String configFileName, Integer redundancy, Collection<LocalDate> competitionDatesFlat) {
		List<LocalDate> competionDateLatestBlock =
			CollectionUtils.INSTANCE.toSubLists(
				new ArrayList<>(competitionDatesFlat),
				redundancy
			).stream().reduce((prev, next) -> next).orElse(null);
		boolean isSlave = CollectionUtils.INSTANCE.retrieveBoolean(config, "simulation.slave", false);
		readOrCreateExcel(
			excelFileName,
			workBook -> {
				Sheet sheet = workBook.getSheet(RESULTS_LABEL);
				Iterator<Row> rowIterator = sortRowsForDate(sheet).iterator();
				Map<String, List<Row>> groupedForRedundancyRows = new LinkedHashMap<>();
				while (rowIterator.hasNext()) {
					Row row = rowIterator.next();
					if (rowRefersTo(row, configFileName)) {
						groupedForRedundancyRows.computeIfAbsent(
							row.getCell(getCellIndex(sheet, FILE_LABEL)).getStringCellValue(),
							key -> new ArrayList<>()
						).add(row);
					}
				}
				List<List<Row>> allGroupedForRedundancyRows = new ArrayList<>(groupedForRedundancyRows.values());
				List<Row> latestGroupOfRows = allGroupedForRedundancyRows.stream().reduce((prev, next) -> next).orElse(null);
				allGroupedForRedundancyRows.remove(latestGroupOfRows);
				List<Row> toBeRemoved = new ArrayList<>();
				for (List<Row> rows : allGroupedForRedundancyRows) {
					if (rows.size() < redundancy) {
						for (Row row : rows) {
							toBeRemoved.add(row);
						}
					}
				}
				if (latestGroupOfRows != null && latestGroupOfRows.size() != redundancy && latestGroupOfRows.size() != competionDateLatestBlock.size()) {
					for (Row row : latestGroupOfRows) {
						toBeRemoved.add(row);
					}
				}
				LogUtils fileLogger = LogUtils.ToFile.getLogger(CollectionUtils.INSTANCE.retrieveValue(config, "logger.file.name"));
				removeRows(
					toBeRemoved,
					rowsForDateComparator,
					row -> rowIndex -> exception -> {
						int rowNum = rowIndex + 1;
						if (exception == null) {
							fileLogger.warn("Row " + rowNum + " of file " + excelFileName + " has been removed");
							LogUtils.INSTANCE.warn("Row " + rowNum + " of file " + excelFileName + " has been removed");
						} else {
							fileLogger.error("Unable to remove row " + rowNum + " for file " + excelFileName + ": " + exception.getMessage());
							LogUtils.INSTANCE.error("Unable to remove row " + rowNum + " for file " + excelFileName + ": " + exception.getMessage());
						}
					}
				);
			},
			workBook ->
				createWorkbook(workBook, excelFileName),
			workBook -> {
				if (!isSlave) {
					store(excelFileName, workBook);
				}
			},
			isSlave
		);
	}

	static List<Row> sortRowsForDate(Sheet sheet) {
		return sortRowsForDate(sheet, false);
	}

	static List<Row> sortRowsForDate(Sheet sheet, boolean reversed) {
		Iterator<Row> rowIterator = sheet.rowIterator();
		for (int i = 0; i < header.size(); i++) {
			rowIterator.next();
		}
		List<Row> orderedForDateRows = new ArrayList<>();
		while (rowIterator.hasNext()) {
			orderedForDateRows.add(rowIterator.next());
		}
		return sortRows(orderedForDateRows, rowsForDateComparator, reversed);
	}

	private static boolean rowRefersTo(Row row, String configurationName) {
		Matcher matcher = regexForExtractConfigFileName.matcher(
			row.getCell(getOrPutAndGetCellIndex(row.getSheet(), FILE_LABEL)).getStringCellValue()
		);
		return matcher.find() && matcher.group(1).equals(configurationName);
	}

	protected static void process(
		Properties config,
		String excelFileName,
		SELotteryMatrixGeneratorEngine engine,
		List<List<LocalDate>> competitionDates,
		AtomicBoolean firstSetupExecuted
	) {
		String redundantConfigValue = CollectionUtils.INSTANCE.retrieveValue(config, "simulation.redundancy");
		boolean isSlave = CollectionUtils.INSTANCE.retrieveBoolean(config, "simulation.slave", false);
		Function<LocalDate, Function<List<Storage>, Integer>> extractionDatePredicate = null;
		Function<LocalDate, Consumer<List<Storage>>> systemProcessor = null;
		AtomicInteger redundantCounter = new AtomicInteger(0);
		if (isSlave) {
			if (redundantConfigValue != null) {
				for (
					List<LocalDate> datesToBeProcessed :
					competitionDates
				) {
					LocalDate date = datesToBeProcessed.get(0);
					datesToBeProcessed.clear();
					datesToBeProcessed.add(date);
				}
			}
			Collections.shuffle(competitionDates);
		} else {
			extractionDatePredicate = buildExtractionDatePredicate(
				config,
				excelFileName,
				redundantConfigValue != null? Integer.valueOf(redundantConfigValue) : null,
				redundantCounter
			);
			systemProcessor = buildSystemProcessor(config, excelFileName);
		}
		//Per cachare il motore in caso di utilizzo dell'opzione prevSys
		engine.setup(config, true);
		checkAndNotifyExecutionOfFirstSetupForConfiguration(firstSetupExecuted);
		for (
			List<LocalDate> datesToBeProcessed :
			competitionDates
		) {
			config.setProperty("competition",
				String.join(",",
					datesToBeProcessed.stream().map(TimeUtils.defaultLocalDateFormat::format).collect(Collectors.toList())
				)
			);
			engine.setup(config, false).getExecutor().apply(
				extractionDatePredicate
			).apply(
				systemProcessor
			);
		}		updateHistorical(config, excelFileName);
		if (!isSlave) {
			readOrCreateExcel(
				excelFileName,
				workBook -> {
					SimpleWorkbookTemplate workBookTemplate = new SimpleWorkbookTemplate(workBook);
					Sheet sheet = workBookTemplate.getOrCreateSheet(RESULTS_LABEL, true);
					workBookTemplate.setAutoFilter(1, getMaxRowIndex(), 0, reportHeaderLabels.size() - 1);
					workBookTemplate.addSheetConditionalFormatting(
						new int[] {
							getOrPutAndGetCellIndex(sheet, Premium.LABEL_FIVE),
							getOrPutAndGetCellIndex(sheet, getFollowingProgressiveHistoricalPremiumLabel(Premium.LABEL_FIVE))
						},
						IndexedColors.YELLOW,
						ComparisonOperator.GT,
						getHeaderSize(),
						sh -> sh.getLastRowNum() + 1,
						"0"
					);
					workBookTemplate.addSheetConditionalFormatting(
						new int[] {
							getOrPutAndGetCellIndex(sheet, Premium.LABEL_FIVE_PLUS),
							getOrPutAndGetCellIndex(sheet, getFollowingProgressiveHistoricalPremiumLabel(Premium.LABEL_FIVE_PLUS))
						},
						IndexedColors.ORANGE,
						ComparisonOperator.GT,
						getHeaderSize(),
						sh -> sh.getLastRowNum() + 1,
						"0"
					);
					workBookTemplate.addSheetConditionalFormatting(
						new int[] {
							getOrPutAndGetCellIndex(sheet, Premium.LABEL_SIX),
							getOrPutAndGetCellIndex(sheet, getFollowingProgressiveHistoricalPremiumLabel(Premium.LABEL_SIX))
						},
						IndexedColors.RED,
						ComparisonOperator.GT,
						getHeaderSize(),
						sh -> sh.getLastRowNum() + 1,
						"0"
					);
				},
				null,
				workBook -> {
					if (!isSlave) {
						store(excelFileName, workBook);
					}
				},
				isSlave
			);
			if (!isSlave) {
				backup(new File(PersistentStorage.buildWorkingPath() + File.separator + excelFileName));
			}
		}
		//Puliamo file txt duplicati da google drive
		for (File file : ResourceUtils.INSTANCE.find("(1)", "txt", retrieveDataBasePath(config))) {
			file.delete();
		}
		//Puliamo file json duplicati da google drive
		for (File file : ResourceUtils.INSTANCE.find("(1)", "json", retrieveDataBasePath(config))) {
			file.delete();
		}
	}

	protected static void checkAndNotifyExecutionOfFirstSetupForConfiguration(AtomicBoolean firstSetupExecuted) {
		if (!firstSetupExecuted.get()) {
			firstSetupExecuted.set(true);
			synchronized(firstSetupExecuted) {
				firstSetupExecuted.notify();
			}
		}
	}

	private static int getMaxRowIndex() {
		return getMaxRowIndexInExcelFormat() - 1;
	}

	private static int getMaxRowIndexInExcelFormat() {
		return SpreadsheetVersion.EXCEL2007.getMaxRows();
		//return getSEStats().getAllWinningCombos().size() * 2;
	}

	private static Integer updateHistorical(
		Properties config,
		String excelFileName
	) {
		String configurationName = CollectionUtils.INSTANCE.retrieveValue(config, "nameSuffix");
		Map<String, Map<String, Object>> premiumCountersForFile = new LinkedHashMap<>();
		List<Number> premiumTypeList = parseReportWinningInfoConfig(CollectionUtils.INSTANCE.retrieveValue(config, "report.winning-info", "all").replaceAll("\\s+",""));
		Number[] premiumTypes = premiumTypeList.toArray(new Number[premiumTypeList.size()]);
		boolean isSlave = CollectionUtils.INSTANCE.retrieveBoolean(config, "simulation.slave", false);
		SEStats sEStats = getSEStats(config);
		Function<Date, Date> dateOffsetComputer = dateOffsetComputer(config);
		AtomicReference<Integer> removedRowResult = new AtomicReference<>();
		Integer result = readOrCreateExcel(
			excelFileName,
			workBook -> {
				Sheet sheet = workBook.getSheet(RESULTS_LABEL);
				if (sheet.getPhysicalNumberOfRows() < 3) {
					return;
				}
				Integer extractionDateColIndex = getOrPutAndGetCellIndex(sheet, EXTRACTION_DATE_LABEL);
				Integer dataAggStoricoColIndex = getOrPutAndGetCellIndex(sheet, HISTORICAL_UPDATE_DATE_LABEL);
				Integer costColIndex = getOrPutAndGetCellIndex(sheet, COST_LABEL);
				Integer historicalCostColIndex = getOrPutAndGetCellIndex(sheet, HISTORICAL_COST_LABEL);
				Integer historicalReturnColIndex = getOrPutAndGetCellIndex(sheet, HISTORICAL_RETURN_LABEL);
				Integer followingProgressiveHistoricalCostColIndex = getOrPutAndGetCellIndex(sheet, FOLLOWING_PROGRESSIVE_HISTORICAL_COST_LABEL);
				Integer followingProgressiveHistoricalReturnColIndex = getOrPutAndGetCellIndex(sheet, FOLLOWING_PROGRESSIVE_HISTORICAL_RETURN_LABEL);
				Integer fileColIndex = getOrPutAndGetCellIndex(sheet, FILE_LABEL);
				Row firstRow = getFirstRow(sheet);
				CellStyle dateCellStyle = firstRow.getCell(extractionDateColIndex).getCellStyle();
				CellStyle numberCellStyle = firstRow.getCell(costColIndex).getCellStyle();
				CellStyle hyperLinkNumberCellStyle = null;
				if (firstRow.getCell(getOrPutAndGetCellIndex(sheet, getFollowingProgressiveHistoricalPremiumLabel(Premium.LABEL_TWO))).getCellType() == CellType.BLANK) {
					hyperLinkNumberCellStyle = workBook.createCellStyle();
					hyperLinkNumberCellStyle.cloneStyleFrom(firstRow.getCell(fileColIndex).getCellStyle());
					hyperLinkNumberCellStyle.setDataFormat(workBook.createDataFormat().getFormat("#,##0"));
					hyperLinkNumberCellStyle.setAlignment(HorizontalAlignment.RIGHT);
				} else {
					hyperLinkNumberCellStyle = firstRow.getCell(historicalReturnColIndex).getCellStyle();
				}
				List<Integer> rowsToBeProcessed = IntStream.range(getHeaderSize(), sheet.getPhysicalNumberOfRows()).boxed().collect(Collectors.toList());
				if (isSlave) {
					Collections.shuffle(rowsToBeProcessed);
				}
				int rowProcessedCounter = 0;
				int modifiedRowCounter = 0;
				List<Row> rowsToBeRemoved = new ArrayList<>();
				for (int index = 0; index < rowsToBeProcessed.size(); index++) {
					Integer rowIndex = rowsToBeProcessed.get(index);
					Row currentRow = sheet.getRow(rowIndex);
					try {
						currentRow.getCell(historicalReturnColIndex).setCellStyle(hyperLinkNumberCellStyle);
						if (rowRefersTo(currentRow, configurationName)) {
							Date dataAggStor = dateOffsetComputer.apply(currentRow.getCell(dataAggStoricoColIndex).getDateCellValue());
							if (dataAggStor == null || dataAggStor.compareTo(sEStats.getLatestExtractionDate()) < 0) {
								Map.Entry<Integer, Date> rowIndexAndExtractionDate = new AbstractMap.SimpleEntry<>(rowIndex, currentRow.getCell(extractionDateColIndex).getDateCellValue());
								AtomicReference<PersistentStorage> storageWrapper = new AtomicReference<>();
								storageWrapper.set(
									PersistentStorage.restore(
										CollectionUtils.INSTANCE.retrieveValue(config, "group"),
										currentRow.getCell(fileColIndex).getStringCellValue()
									)
								);
								String extractionDateFormattedForFile = TimeUtils.getDefaultDateFmtForFilePrefix().format(rowIndexAndExtractionDate.getValue());
								if (storageWrapper.get() != null) {
									Map<String, Object> premiumCountersData = premiumCountersForFile.computeIfAbsent(storageWrapper.get().getName() + extractionDateFormattedForFile, key -> {
										PersistentStorage storage = storageWrapper.get();
										File premiumCountersFile = new File(
											new File(
											storage.getAbsolutePath()).getParentFile().getAbsolutePath() + File.separator + storage.getNameWithoutExtension() +
											"-historical-data" +
											extractionDateFormattedForFile + ".json"
										);
										if (!premiumCountersFile.exists()) {
											return computePremiumCountersData(sEStats, storage, rowIndexAndExtractionDate.getValue(), premiumCountersFile, premiumTypes);
										} else {
											Map<String, Object> data = null;
											try {
												data = readPremiumCountersData(storage, rowIndexAndExtractionDate.getValue(), premiumCountersFile);
											} catch (IOException exc) {
												LogUtils.INSTANCE.error("Unable to read file " + premiumCountersFile.getAbsolutePath() + ": it will be deleted and recreated");
												if (!premiumCountersFile.delete()) {
													Throwables.INSTANCE.throwException(exc);
												}
												return computePremiumCountersData(sEStats, storage, rowIndexAndExtractionDate.getValue(), premiumCountersFile, premiumTypes);
											}
											try {
												if (dateOffsetComputer.apply(TimeUtils.getDefaultDateFormat().parse((String)data.get("referenceDate")))
													.compareTo(sEStats.getLatestExtractionDate()) < 0
												) {
													return computePremiumCountersData(sEStats, storage, rowIndexAndExtractionDate.getValue(), premiumCountersFile, premiumTypes);
												}
											} catch (ParseException exc) {
												return Throwables.INSTANCE.throwException(exc);
											}
											return data;
										}
									});
									if (!isSlave) {
										++modifiedRowCounter;
										Storage storage = storageWrapper.get();
										if (storage.getName().equals(currentRow.getCell(fileColIndex).getStringCellValue())) {
											Cell dataAggStoricoCell = currentRow.getCell(dataAggStoricoColIndex);
											for (Map.Entry<Number, String> premiumData :  Premium.all().entrySet()) {
												Cell historyDataCell =
													currentRow.getCell(getOrPutAndGetCellIndex(sheet, getHistoryPremiumLabel(premiumData.getValue())));
												Number premiumCounter = ((Map<Number,Integer>)premiumCountersData.get("premiumCounters.all")).get(premiumData.getKey());
												if (premiumCounter != null) {
													historyDataCell.setCellValue(premiumCounter.doubleValue());
												} else {
													historyDataCell.setCellValue(0d);
												}
												historyDataCell =
													currentRow.getCell(getOrPutAndGetCellIndex(sheet, getFollowingProgressiveHistoricalPremiumLabel(premiumData.getValue())));
												premiumCounter = ((Map<Number,Integer>)premiumCountersData.get("premiumCounters.fromExtractionDate")).get(premiumData.getKey());
												if (premiumCounter != null) {
													historyDataCell.setCellValue(premiumCounter.doubleValue());
												} else {
													historyDataCell.setCellValue(0d);
												}
												historyDataCell.setCellStyle(numberCellStyle);
											}
											Cell cell = currentRow.getCell(historicalCostColIndex);
											cell.setCellStyle(numberCellStyle);
											cell.setCellValue(
												(Integer)premiumCountersData.get("premiumCounters.all.processedExtractionDateCounter") * currentRow.getCell(costColIndex).getNumericCellValue()
											);
											cell = currentRow.getCell(followingProgressiveHistoricalCostColIndex);
											cell.setCellStyle(numberCellStyle);
											cell.setCellValue(
												(Integer)premiumCountersData.get("premiumCounters.fromExtractionDate.processedExtractionDateCounter") * currentRow.getCell(costColIndex).getNumericCellValue()
											);
											File reportDetailFileFromExtractionDate = (File) premiumCountersData.get("reportDetailFile.fromExtractionDate");
											if (reportDetailFileFromExtractionDate != null) {
												SimpleWorkbookTemplate.setLinkForCell(
													workBook,
													HyperlinkType.FILE,
													currentRow.getCell(followingProgressiveHistoricalReturnColIndex),
													hyperLinkNumberCellStyle,
													reportDetailFileFromExtractionDate.getParentFile().getName() + File.separator + reportDetailFileFromExtractionDate.getName()
												);
											}
											File reportDetailFile = (File) premiumCountersData.get("reportDetailFile.all");
											if (reportDetailFile != null) {
												SimpleWorkbookTemplate.setLinkForCell(
													workBook,
													HyperlinkType.FILE,
													currentRow.getCell(historicalReturnColIndex),
													hyperLinkNumberCellStyle,
													reportDetailFile.getParentFile().getName() + File.separator + reportDetailFile.getName()
												);
											}
											dataAggStoricoCell.setCellStyle(dateCellStyle);
											dataAggStoricoCell.setCellValue(sEStats.getLatestExtractionDate());
										}
										if ((modifiedRowCounter % 10) == 0) {
											LogUtils.INSTANCE.info("Storing historical data of " + excelFileName);
											store(excelFileName, workBook);
										}
									}
								} else {
									throw new IllegalStateException(currentRow.getCell(getOrPutAndGetCellIndex(sheet, FILE_LABEL)).getStringCellValue() + " missing");
								}
							}
						}
					} catch (Throwable exc) {
						LogUtils.INSTANCE.error("Exception occurred while processing row " + (rowIndex + 1) + " of file " + excelFileName + ": " + exc.getMessage());
						if (!isSlave) {
							LogUtils fileLogger = LogUtils.ToFile.getLogger(CollectionUtils.INSTANCE.retrieveValue(config, "logger.file.name"));
							fileLogger.error("Exception occurred while processing row " + (rowIndex + 1) + " of file " + excelFileName + ": " + exc.getMessage());
							fileLogger.warn("Row " + (rowIndex + 1) + " of file " + excelFileName + " will be removed");
							LogUtils.INSTANCE.warn("Row " + (rowIndex + 1) + " of file " + excelFileName + " will be removed");
							rowsToBeRemoved.add(currentRow);
						}
					}
					++rowProcessedCounter;
					int remainedRecords = (rowsToBeProcessed.size() - (rowProcessedCounter));
					if (remainedRecords % 250 == 0) {
						LogUtils.INSTANCE.info("Historical update remained records of " + excelFileName + ": " + remainedRecords);
					}
				}
				if (!isSlave) {
					LogUtils fileLogger = LogUtils.ToFile.getLogger(CollectionUtils.INSTANCE.retrieveValue(config, "logger.file.name"));
					if (!rowsToBeRemoved.isEmpty()) {
						removeRows(
							rowsToBeRemoved,
							rowsForDateComparator,
							row -> rowIndex -> exception -> {
								int rowNum = rowIndex + 1;
								if (exception == null) {
									fileLogger.warn("Row " + rowNum + " of file " + excelFileName + " has been removed");
									LogUtils.INSTANCE.warn("Row " + rowNum + " of file " + excelFileName + " has been removed");
									store(excelFileName, workBook);
								} else {
									removedRowResult.set(-3);
									fileLogger.error("Unable to remove row " + rowNum + " for file " + excelFileName + ": " + exception.getMessage());
									LogUtils.INSTANCE.error("Unable to remove row " + rowNum + " for file " + excelFileName + ": " + exception.getMessage());
								}
							}
						);
					}
					if (!rowsToBeRemoved.isEmpty()) {
						if (removedRowResult.get() == null) {
							removedRowResult.set(-2);
						}
						fileLogger.error();
					}
				}
			},
			null,
			workBook -> {
				if (!isSlave) {
					LogUtils.INSTANCE.info("Final historical data storing of " + excelFileName);
					store(excelFileName, workBook);
				}
			},
			isSlave
		);
		return removedRowResult.get() == null ?
			result :
			removedRowResult.get();
	}

	protected static Row getFirstRow(Sheet sheet) {
		return sheet.getRow(getHeaderSize());
	}

	protected static int getHeaderSize() {
		return header.size();
	}

	private static List<Number> parseReportWinningInfoConfig(String reportWinningInfoConfig) {
		if (reportWinningInfoConfig.equalsIgnoreCase("all")) {
			return Premium.allTypesList();
		} else if (reportWinningInfoConfig.equalsIgnoreCase("high")) {
			return Premium.allHighTypesList();
		} else {
			List<Number> premiumsTypes = new ArrayList<>();
			for (String configPremiumLabel : reportWinningInfoConfig.split(",")) {
				if (configPremiumLabel.equalsIgnoreCase("high")) {
					premiumsTypes.addAll(Premium.allHighTypesList());
					continue;
				}
				for (String premiumLabel : Premium.allLabelsList()) {
					if (configPremiumLabel.equalsIgnoreCase(premiumLabel.replaceAll("\\s+",""))) {
						premiumsTypes.add(Premium.toType(premiumLabel));
						break;
					}
				}
			}
			return premiumsTypes;
		}
	}

	private static Function<Date, Date> dateOffsetComputer(Properties config) {
		String offsetRaw = CollectionUtils.INSTANCE.retrieveValue(config, "history.validity", "0d");
		String offsetAsString = offsetRaw.replaceAll("\\s+","").split("d|D|w|W|m|M")[0];
		Integer offset = Integer.valueOf(offsetAsString);
		if (offset.compareTo(0) == 0) {
			return date -> date;
		}
		String incrementationType = String.valueOf(offsetRaw.charAt(offsetRaw.length()-1));
		if (incrementationType.equalsIgnoreCase("w")) {
			return date ->
				date != null ?
					TimeUtils.increment(date, offset, ChronoUnit.WEEKS) :
					null;
		} else if (incrementationType.equalsIgnoreCase("m")) {
			return date ->
				date != null ?
					TimeUtils.increment(date, offset, ChronoUnit.MONTHS) :
					null;
		}
		return date ->
			date != null ?
				TimeUtils.increment(date, offset, ChronoUnit.DAYS) :
				null;
	}

	protected static SEStats getSEStats(Properties config) {
		return SEStats.get(
			CollectionUtils.INSTANCE.retrieveValue(config,
				"competition.archive.start-date",
				new SELotteryMatrixGeneratorEngine().getDefaultExtractionArchiveStartDate()
			), TimeUtils.defaultLocalDateFormat.format(TimeUtils.today())
		);
	}

	protected static Map<String, Object> readPremiumCountersData(
		PersistentStorage storage,
		Date extractionDate,
		File premiumCountersFile
	) throws StreamReadException, DatabindException, IOException {
		Map<String, Object> data = IOUtils.INSTANCE.readFromJSONFormat(premiumCountersFile, Map.class);
		data.put("premiumCounters.all",((Map<String, Integer>)data.get("premiumCounters.all")).entrySet().stream()
			.collect(Collectors.toMap(entry -> Premium.parseType(entry.getKey()), Map.Entry::getValue, (x, y) -> y, LinkedHashMap::new)));
		data.put("premiumCounters.fromExtractionDate",((Map<String, Integer>)data.get("premiumCounters.fromExtractionDate")).entrySet().stream()
			.collect(Collectors.toMap(entry ->Premium.parseType(entry.getKey()), Map.Entry::getValue, (x, y) -> y, LinkedHashMap::new)));
		String basePath = new File(storage.getAbsolutePath()).getParentFile().getAbsolutePath() + File.separator + storage.getNameWithoutExtension();

		File reportDetailFile = new File(basePath + "-historical-premiums.txt");
		if (reportDetailFile.exists()) {
			data.put("reportDetailFile.all", reportDetailFile);
		}
		File reportDetailFileFromExtractionDate = new File(
			basePath + "-historical-premiums" +
			TimeUtils.getDefaultDateFmtForFilePrefix().format(extractionDate) + ".txt"
		);
		if (reportDetailFileFromExtractionDate.exists()) {
			data.put("reportDetailFile.fromExtractionDate", reportDetailFileFromExtractionDate);
		}
		return data;
	}

	protected static Map<String, Object> computePremiumCountersData(
		SEStats sEStats,
		PersistentStorage storage,
		Date extractionDate,
		File premiumCountersFile,
		Number... premiumTypes
	) {
		//LogUtils.INSTANCE.info("Computing historycal data of " + storage.getName());
		Map<String, Object> qualityCheckResult =
			sEStats.checkQuality(storage::iterator, Premium.allTypes(), premiumTypes);
		Map<String, Object> qualityCheckResultFromExtractionDate =
			sEStats.checkQualityFrom(storage::iterator, extractionDate,  Premium.allTypes(), premiumTypes);
		String reportDetail = (String)qualityCheckResult.get("report.detail");
		String reportDetailFromExtractionDate = (String)qualityCheckResultFromExtractionDate.get("report.detail");
		String basePath = new File(storage.getAbsolutePath()).getParentFile().getAbsolutePath() + File.separator + storage.getNameWithoutExtension();
		File reportDetailFile = IOUtils.INSTANCE.writeToNewFile(basePath + "-historical-premiums.txt", reportDetail);
		File reportDetailFileFromExtractionDate = IOUtils.INSTANCE.writeToNewFile(
			basePath + "-historical-premiums" +
			TimeUtils.getDefaultDateFmtForFilePrefix().format(extractionDate) + ".txt", reportDetailFromExtractionDate
		);
		LogUtils.INSTANCE.info("Computed historycal data of " + storage.getName());
		Map<String, Object> data = new LinkedHashMap<>();
		data.put("premiumCounters.all", qualityCheckResult.get("premium.counters"));
		data.put("premiumCounters.all.processedExtractionDateCounter", qualityCheckResult.get("processedExtractionDateCounter"));
		data.put("premiumCounters.fromExtractionDate", qualityCheckResultFromExtractionDate.get("premium.counters"));
		data.put("premiumCounters.fromExtractionDate.processedExtractionDateCounter", qualityCheckResultFromExtractionDate.get("processedExtractionDateCounter"));
		data.put("referenceDate", qualityCheckResult.get("referenceDate"));
		IOUtils.INSTANCE.writeToJSONFormat(premiumCountersFile, data);
		data.put("reportDetailFile.all", reportDetailFile);
		data.put("reportDetailFile.fromExtractionDate", reportDetailFileFromExtractionDate);
		return data;
	}

	private static Function<LocalDate, Consumer<List<Storage>>> buildSystemProcessor(Properties configuration, String excelFileName) {
		AtomicBoolean rowAddedFlag = new AtomicBoolean(false);
		AtomicBoolean fileCreatedFlag = new AtomicBoolean(false);
		boolean isSlave = CollectionUtils.INSTANCE.retrieveBoolean(configuration, "simulation.slave", false);
		return extractionDate -> storages -> {
			readOrCreateExcel(
				excelFileName,
				workBook -> {
					SimpleWorkbookTemplate workBookTemplate = new SimpleWorkbookTemplate(workBook);
					Sheet sheet = workBook.getSheet(RESULTS_LABEL);
					Storage storage = !storages.isEmpty() ? storages.get(storages.size() -1) : null;
					if (storage != null) {
						Row row = sheet.createRow(sheet.getPhysicalNumberOfRows());
						addRowData(row, extractionDate, (PersistentStorage)storage);
						rowAddedFlag.set(true);
					}
				},
				workBook -> {
					createWorkbook(workBook, excelFileName);
					fileCreatedFlag.set(true);
				},
				workBook -> {
					if (!isSlave && (fileCreatedFlag.get() || rowAddedFlag.get())) {
						store(excelFileName, workBook);
					}
				},
				isSlave
			);
		};
	}

	protected static void addRowData(
		Row row,
		LocalDate extractionDate,
		PersistentStorage storage
	) throws UnsupportedEncodingException {
		Sheet sheet = row.getSheet();
		Workbook workBook = sheet.getWorkbook();
		CellStyle numberCellStyle = null;
		CellStyle dateCellStyle = null;
		CellStyle hyperLinkStyle = null;
		Integer currentRowNum = row.getRowNum();
		if (currentRowNum > getHeaderSize()) {
			Row firstRow = getFirstRow(sheet);
			numberCellStyle = firstRow.getCell(getOrPutAndGetCellIndex(sheet, BALANCE_LABEL)).getCellStyle();
			dateCellStyle = firstRow.getCell(getOrPutAndGetCellIndex(sheet, EXTRACTION_DATE_LABEL)).getCellStyle();
			hyperLinkStyle = firstRow.getCell(getOrPutAndGetCellIndex(sheet, FILE_LABEL)).getCellStyle();
		} else {
			numberCellStyle = workBook.createCellStyle();
			numberCellStyle.setAlignment(HorizontalAlignment.RIGHT);
			DataFormat dataFormat = workBook.createDataFormat();
			numberCellStyle.setDataFormat(dataFormat.getFormat("#,##0"));

			dateCellStyle = workBook.createCellStyle();
			dateCellStyle.setAlignment(HorizontalAlignment.CENTER);
			dateCellStyle.setDataFormat(dataFormat.getFormat("dd/MM/yyyy"));

			hyperLinkStyle = workBook.createCellStyle();
			Font fontStyle = workBook.createFont();
			fontStyle.setColor(IndexedColors.BLUE.index);
			fontStyle.setUnderline(XSSFFont.U_SINGLE);
			hyperLinkStyle.setFont(fontStyle);
		}
		Map<String, Integer> results = allTimeStats.checkFor(extractionDate, storage::iterator);
		Cell cell = row.createCell(getOrPutAndGetCellIndex(row.getSheet(), EXTRACTION_DATE_LABEL));
		cell.setCellStyle(dateCellStyle);
		cell.setCellValue(TimeUtils.toDate(extractionDate));

		List<String> allPremiumLabels = Premium.allLabelsList();
		for (int i = 0; i < allPremiumLabels.size();i++) {
			String label = allPremiumLabels.get(i);
			Integer result = results.get(allPremiumLabels.get(i));
			if (result == null) {
				result = 0;
			}
			cell = row.createCell(getOrPutAndGetCellIndex(row.getSheet(), label));
			cell.setCellStyle(numberCellStyle);
			cell.setCellValue(result);
		}
		cell = row.createCell(getOrPutAndGetCellIndex(row.getSheet(), COST_LABEL));
		cell.setCellStyle(numberCellStyle);
		cell.setCellValue(storage.size());

		List<String> allFormulas = Premium.allLabelsList().stream().map(
			label -> generatePremiumFormula(sheet, currentRowNum, label, lb -> lb)).collect(Collectors.toList());
		String formula = String.join("+", allFormulas);
		cell = row.createCell(getOrPutAndGetCellIndex(row.getSheet(), RETURN_LABEL));
		cell.setCellStyle(numberCellStyle);
		cell.setCellFormula(formula);

		formula = generateBalanceFormula(currentRowNum, sheet, Arrays.asList(RETURN_LABEL, COST_LABEL));
		cell = row.createCell(getOrPutAndGetCellIndex(row.getSheet(), BALANCE_LABEL));
		cell.setCellStyle(numberCellStyle);
		cell.setCellFormula(formula);

		for (int i = 0; i < allPremiumLabels.size();i++) {
			cell = row.createCell(getOrPutAndGetCellIndex(sheet, getFollowingProgressiveHistoricalPremiumLabel(allPremiumLabels.get(i))));
			cell.setCellStyle(numberCellStyle);
		}
		cell = row.createCell(getOrPutAndGetCellIndex(sheet, FOLLOWING_PROGRESSIVE_HISTORICAL_COST_LABEL));
		cell.setCellStyle(numberCellStyle);
		cell.setCellValue(0);

		allFormulas = Premium.allLabelsList().stream().map(
				label -> generatePremiumFormula(sheet, currentRowNum, label, SELotterySimpleSimulator::getFollowingProgressiveHistoricalPremiumLabel)).collect(Collectors.toList());
		formula = String.join("+", allFormulas);
		cell = row.createCell(getOrPutAndGetCellIndex(sheet, FOLLOWING_PROGRESSIVE_HISTORICAL_RETURN_LABEL));
		cell.setCellStyle(numberCellStyle);
		cell.setCellFormula(formula);

		formula = generateBalanceFormula(currentRowNum, sheet, Arrays.asList(FOLLOWING_PROGRESSIVE_HISTORICAL_RETURN_LABEL, FOLLOWING_PROGRESSIVE_HISTORICAL_COST_LABEL));
		cell = row.createCell(getOrPutAndGetCellIndex(sheet, FOLLOWING_PROGRESSIVE_HISTORICAL_BALANCE_LABEL));
		cell.setCellStyle(numberCellStyle);
		cell.setCellFormula(formula);

		for (int i = 0; i < allPremiumLabels.size();i++) {
			cell = row.createCell(getOrPutAndGetCellIndex(sheet, getHistoryPremiumLabel(allPremiumLabels.get(i))));
			cell.setCellStyle(numberCellStyle);
		}

		cell = row.createCell(getOrPutAndGetCellIndex(sheet, HISTORICAL_COST_LABEL));
		cell.setCellStyle(numberCellStyle);
		cell.setCellValue(0);

		allFormulas = Premium.allLabelsList().stream().map(
				label -> generatePremiumFormula(sheet, currentRowNum, label, SELotterySimpleSimulator::getHistoryPremiumLabel)).collect(Collectors.toList());
		formula = String.join("+", allFormulas);
		cell = row.createCell(getOrPutAndGetCellIndex(sheet, HISTORICAL_RETURN_LABEL));
		cell.setCellStyle(numberCellStyle);
		cell.setCellFormula(formula);

		formula = generateBalanceFormula(currentRowNum, sheet, Arrays.asList(HISTORICAL_RETURN_LABEL, HISTORICAL_COST_LABEL));
		cell = row.createCell(getOrPutAndGetCellIndex(sheet, HISTORICAL_BALANCE_LABEL));
		cell.setCellStyle(numberCellStyle);
		cell.setCellFormula(formula);

		cell = row.createCell(getOrPutAndGetCellIndex(row.getSheet(), HISTORICAL_UPDATE_DATE_LABEL));
		cell.setCellStyle(dateCellStyle);

		Hyperlink hyperLink = workBook.getCreationHelper().createHyperlink(HyperlinkType.FILE);
		try {
			hyperLink.setAddress(URLEncoder.encode(new File(storage.getAbsolutePath()).getParentFile().getName() + File.separator + storage.getName(), "UTF-8").replace("+", "%20"));
		} catch (UnsupportedEncodingException exc) {
			Throwables.INSTANCE.throwException(exc);
		}
		cell = row.createCell(getOrPutAndGetCellIndex(row.getSheet(), FILE_LABEL));
		cell.setCellValue(storage.getName());
		cell.setHyperlink(hyperLink);
		cell.setCellStyle(hyperLinkStyle);
	}

	protected static String generateBalanceFormula(int currentRowNum, Sheet sheet, List<String> labels) {
		int effectiveRowIndex = currentRowNum + 1;
		return String.join("-", labels.stream().map(label ->
			"(" + convertNumToColString(getOrPutAndGetCellIndex(sheet, label))  + effectiveRowIndex + ")"
		).collect(Collectors.toList()));
	}

	protected static String generatePremiumFormula(Sheet sheet, int currentRowNum, String columnLabel, UnaryOperator<String> transformer) {
		int effectiveRowIndex = currentRowNum + 1;
		return "(" + convertNumToColString(getOrPutAndGetCellIndex(sheet, transformer.apply(columnLabel))) + effectiveRowIndex + "*" + SEStats.premiumPrice(columnLabel) + ")";
	}

	protected static String getHistoryPremiumLabel(String label) {
		return "Totale " + label.toLowerCase() + " " + HISTORICAL_LABEL;
	}

	protected static String getFollowingProgressiveHistoricalPremiumLabel(String label) {
		return "Totale " + label.toLowerCase() + " " + FOLLOWING_PROGRESSIVE_HISTORICAL_LABEL;
	}

	protected static Function<LocalDate, Function<List<Storage>, Integer>> buildExtractionDatePredicate(
		Properties configuration,
		String excelFileName,
		Integer redundant,
		AtomicInteger redundantCounter
	) {
		String configurationName = configuration.getProperty("nameSuffix");
		boolean isSlave = CollectionUtils.INSTANCE.retrieveBoolean(configuration, "simulation.slave", false);
		return extractionDate -> storages -> {
			AtomicReference<Integer> checkResult = new AtomicReference<Integer>();
			readOrCreateExcel(
				excelFileName,
				workBook ->
					checkResult.set(checkAlreadyProcessed(workBook, configurationName, extractionDate)),
				workBook ->
					createWorkbook(workBook, excelFileName)
				,
				workBook -> {
					if (!isSlave) {
						store(excelFileName, workBook);
					}
				},
				CollectionUtils.INSTANCE.retrieveBoolean(configuration, "simulation.slave", false)
			);
			if (redundant != null) {
				Integer redundantCounterValue = redundantCounter.getAndIncrement();
				if (redundantCounterValue.intValue() > 0) {
					if (redundantCounterValue.intValue() < redundant) {
						checkResult.set(
							checkResult.get() != null ?
								checkResult.get() :
								0
							);
					} else {
						redundantCounter.set(1);
					}
				}
			}
			if (checkResult.get() == null) {
				checkResult.set(1);
			}
			return checkResult.get();
		};
	}

	protected static Integer checkAlreadyProcessed(
		Workbook workBook,
		String configurationName,
		LocalDate extractionDate
	) {
		Iterator<Row> rowIterator = workBook.getSheet(RESULTS_LABEL).rowIterator();
		rowIterator.next();
		rowIterator.next();
		while (rowIterator.hasNext()) {
			Row row = rowIterator.next();
			if (rowRefersTo(row, configurationName)) {
				Cell data = row.getCell(0);
				if (data != null && extractionDate.compareTo(TimeUtils.toLocalDate(data.getDateCellValue())) == 0) {
					return -1;
				}
			}
		}
		return null;
	}

	protected static void createWorkbook(Workbook workBook, String excelFileName) {
		SimpleWorkbookTemplate workBookTemplate = new SimpleWorkbookTemplate(workBook);
		Sheet sheet = workBookTemplate.getOrCreateSheet(RESULTS_LABEL, true);

		workBookTemplate.createHeader(
			RESULTS_LABEL,
			true,
			header
		);
		CellStyle headerNumberStyle = workBook.createCellStyle();
		headerNumberStyle.cloneStyleFrom(sheet.getRow(1).getCell(getOrPutAndGetCellIndex(sheet, COST_LABEL)).getCellStyle());
		headerNumberStyle.setDataFormat(workBook.createDataFormat().getFormat("#,##0"));
		for (String label : Premium.allLabelsList()) {
			sheet.getRow(1).getCell(getOrPutAndGetCellIndex(sheet, label)).setCellStyle(headerNumberStyle);
			sheet.getRow(1).getCell(getOrPutAndGetCellIndex(sheet, getFollowingProgressiveHistoricalPremiumLabel(label))).setCellStyle(headerNumberStyle);
			sheet.getRow(1).getCell(getOrPutAndGetCellIndex(sheet, getHistoryPremiumLabel(label))).setCellStyle(headerNumberStyle);
		}
		sheet.getRow(1).getCell(getOrPutAndGetCellIndex(sheet, COST_LABEL)).setCellStyle(headerNumberStyle);
		sheet.getRow(1).getCell(getOrPutAndGetCellIndex(sheet, RETURN_LABEL)).setCellStyle(headerNumberStyle);
		sheet.getRow(1).getCell(getOrPutAndGetCellIndex(sheet, BALANCE_LABEL)).setCellStyle(headerNumberStyle);
		sheet.getRow(1).getCell(getOrPutAndGetCellIndex(sheet, BALANCE_LABEL)).setCellFormula(
			"TEXT((SUM(" + convertNumToColString(reportHeaderLabels.indexOf(BALANCE_LABEL)) + (getHeaderSize() + 1) + ":" +
			convertNumToColString(reportHeaderLabels.indexOf(BALANCE_LABEL)) + getMaxRowIndexInExcelFormat() +
			")/" + convertNumToColString(reportHeaderLabels.indexOf(COST_LABEL)) + getHeaderSize() + "),\"###,00%\")"
		);
		sheet.getRow(1).getCell(getOrPutAndGetCellIndex(sheet, FOLLOWING_PROGRESSIVE_HISTORICAL_COST_LABEL)).setCellStyle(headerNumberStyle);
		sheet.getRow(1).getCell(getOrPutAndGetCellIndex(sheet, FOLLOWING_PROGRESSIVE_HISTORICAL_RETURN_LABEL)).setCellStyle(headerNumberStyle);
		sheet.getRow(1).getCell(getOrPutAndGetCellIndex(sheet, FOLLOWING_PROGRESSIVE_HISTORICAL_BALANCE_LABEL)).setCellStyle(headerNumberStyle);
		sheet.getRow(1).getCell(getOrPutAndGetCellIndex(sheet, FOLLOWING_PROGRESSIVE_HISTORICAL_BALANCE_LABEL)).setCellFormula(
			"TEXT((SUM(" + convertNumToColString(reportHeaderLabels.indexOf(FOLLOWING_PROGRESSIVE_HISTORICAL_BALANCE_LABEL)) + "3:" +
			convertNumToColString(reportHeaderLabels.indexOf(FOLLOWING_PROGRESSIVE_HISTORICAL_BALANCE_LABEL)) + getMaxRowIndexInExcelFormat() +
			")/" + convertNumToColString(reportHeaderLabels.indexOf(FOLLOWING_PROGRESSIVE_HISTORICAL_COST_LABEL)) + "2),\"###,00%\")"
		);
		sheet.getRow(1).getCell(getOrPutAndGetCellIndex(sheet, HISTORICAL_COST_LABEL)).setCellStyle(headerNumberStyle);
		sheet.getRow(1).getCell(getOrPutAndGetCellIndex(sheet, HISTORICAL_RETURN_LABEL)).setCellStyle(headerNumberStyle);
		sheet.getRow(1).getCell(getOrPutAndGetCellIndex(sheet, HISTORICAL_BALANCE_LABEL)).setCellStyle(headerNumberStyle);
		sheet.getRow(1).getCell(getOrPutAndGetCellIndex(sheet, HISTORICAL_BALANCE_LABEL)).setCellFormula(
			"TEXT((SUM(" + convertNumToColString(reportHeaderLabels.indexOf(HISTORICAL_BALANCE_LABEL)) + (getHeaderSize() + 1) + ":" +
			convertNumToColString(reportHeaderLabels.indexOf(HISTORICAL_BALANCE_LABEL)) + getMaxRowIndexInExcelFormat() +
			")/" + convertNumToColString(reportHeaderLabels.indexOf(HISTORICAL_COST_LABEL)) + getHeaderSize() + "),\"###,00%\")"
		);
		sheet.setColumnWidth(getOrPutAndGetCellIndex(sheet, EXTRACTION_DATE_LABEL), COLUMN_SIZE[1]);
		sheet.setColumnWidth(getOrPutAndGetCellIndex(sheet, COST_LABEL), COLUMN_SIZE[0]);
		sheet.setColumnWidth(getOrPutAndGetCellIndex(sheet, RETURN_LABEL), COLUMN_SIZE[0]);
		sheet.setColumnWidth(getOrPutAndGetCellIndex(sheet, BALANCE_LABEL), COLUMN_SIZE[0]);
		sheet.setColumnWidth(getOrPutAndGetCellIndex(sheet, FOLLOWING_PROGRESSIVE_HISTORICAL_COST_LABEL), COLUMN_SIZE[0]);
		sheet.setColumnWidth(getOrPutAndGetCellIndex(sheet, FOLLOWING_PROGRESSIVE_HISTORICAL_RETURN_LABEL), COLUMN_SIZE[0]);
		sheet.setColumnWidth(getOrPutAndGetCellIndex(sheet, FOLLOWING_PROGRESSIVE_HISTORICAL_BALANCE_LABEL), COLUMN_SIZE[0]);
		sheet.setColumnWidth(getOrPutAndGetCellIndex(sheet, HISTORICAL_COST_LABEL), COLUMN_SIZE[0]);
		sheet.setColumnWidth(getOrPutAndGetCellIndex(sheet, HISTORICAL_RETURN_LABEL), COLUMN_SIZE[0]);
		sheet.setColumnWidth(getOrPutAndGetCellIndex(sheet, HISTORICAL_BALANCE_LABEL), COLUMN_SIZE[0]);
		sheet.setColumnWidth(getOrPutAndGetCellIndex(sheet, HISTORICAL_UPDATE_DATE_LABEL), COLUMN_SIZE[1]);
		sheet.setColumnWidth(getOrPutAndGetCellIndex(sheet, FILE_LABEL), COLUMN_SIZE[2]);
		//LogUtils.INSTANCE.logInfo(PersistentStorage.buildWorkingPath() + File.separator + excelFileName + " succesfully created");
	}

	protected static Integer readOrCreateExcel(
		String excelFileName,
		ThrowingConsumer<Workbook, Throwable> action,
		ThrowingConsumer<Workbook, Throwable> createAction,
		ThrowingConsumer<Workbook, Throwable> finallyAction,
		boolean isSlave
	) {
		return readOrCreateExcelOrComputeBackups(excelFileName, null, action, createAction, finallyAction, 5, isSlave);
	}

	protected static Integer readOrCreateExcelOrComputeBackups(
		String excelFileName,
		Collection<File> backups,
		ThrowingConsumer<Workbook, Throwable> action,
		ThrowingConsumer<Workbook, Throwable> createAction,
		ThrowingConsumer<Workbook, Throwable> finallyAction,
		int slaveAdditionalReadingMaxAttempts,
		boolean isSlave
	) {
		excelFileName = excelFileName.replace("/", File.separator).replace("\\", File.separator);
		String excelFileAbsolutePath = (PersistentStorage.buildWorkingPath() + File.separator + excelFileName).replace("/", File.separator).replace("\\", File.separator);
		try {
			Synchronizer.INSTANCE.executeThrower(excelFileName, () -> {
				Workbook workBook = null;
				try {
					try (InputStream inputStream = new FileInputStream(excelFileAbsolutePath)) {
						workBook = new XSSFWorkbook(inputStream);
						action.accept(workBook);
					} catch (FileNotFoundException exc) {
						if (createAction == null) {
							throw exc;
						}
						workBook = new XSSFWorkbook();
						createAction.accept(workBook);
					}
					if (workBook != null && finallyAction!= null) {
						try {
							finallyAction.accept(workBook);
						} catch (Throwable exc) {
							Throwables.INSTANCE.throwException(exc);
						}
					}
				} finally {
					if (workBook != null) {
						try {
							workBook.close();
						} catch (IOException exc) {
							Throwables.INSTANCE.throwException(exc);
						}
					}
				}
			});
		} catch (Throwable exc) {
			if (!(exc instanceof POIXMLException || exc instanceof EmptyFileException || exc instanceof ZipException || exc instanceof PartAlreadyExistsException || exc instanceof IllegalStateException ||
				exc instanceof NotOfficeXmlFileException || exc instanceof XmlValueDisconnectedException || exc instanceof RecordFormatException || (exc instanceof IOException && exc.getMessage().equalsIgnoreCase("Truncated ZIP file")))) {
				LogUtils.INSTANCE.error("Unable to process file " + excelFileName);
				Throwables.INSTANCE.throwException(exc);
			}
			if (isSlave) {
				Mutex mutex = Synchronizer.INSTANCE.getMutex(excelFileName);
				synchronized(mutex) {
					LogUtils.INSTANCE.error("Error in Excel file '" + excelFileAbsolutePath + "'. Wating for restore by master");
					try {
						if (--slaveAdditionalReadingMaxAttempts > 0) {
							mutex.wait(5000);
						} else {
							LogUtils.INSTANCE.error("Error in Excel file '" + excelFileAbsolutePath + "'. The file will be skipped");
							return -1;
						}
					} catch (InterruptedException e) {
						Throwables.INSTANCE.throwException(e);
					}
				}
			} else {
				String excelFileParentPath = excelFileAbsolutePath.substring(0, excelFileAbsolutePath.lastIndexOf(File.separator));
				String effectiveExcelFileName =  excelFileAbsolutePath.substring(excelFileAbsolutePath.lastIndexOf(File.separator)+1);
				String effectiveExcelFileNameWithoutExtension = effectiveExcelFileName.substring(0, effectiveExcelFileName.lastIndexOf("."));
				String excelFileExtension = effectiveExcelFileName.substring(effectiveExcelFileName.lastIndexOf(".") +1);
				if (backups == null) {
					backups = ResourceUtils.INSTANCE.findReverseOrdered(effectiveExcelFileNameWithoutExtension + " - ", excelFileExtension, excelFileParentPath);
				}
				if (backups.isEmpty()) {
					LogUtils.INSTANCE.error("Error in Excel file '" + excelFileAbsolutePath + "'. No backup found");
					Throwables.INSTANCE.throwException(exc);
				}
				Iterator<File> backupsIterator = backups.iterator();
				File backup = backupsIterator.next();
				LogUtils.INSTANCE.warn("Error in Excel file '" + excelFileAbsolutePath + "'.\nTrying to restore previous backup: '" + backup.getAbsolutePath() + "'");
				File processedFile = new File(excelFileAbsolutePath);
				if (!processedFile.delete() || !backup.renameTo(processedFile)) {
					Throwables.INSTANCE.throwException(exc);
				}
				backupsIterator.remove();
			}
			return readOrCreateExcelOrComputeBackups(excelFileName, backups, action, createAction, finallyAction, slaveAdditionalReadingMaxAttempts, isSlave);
		}
		return 1;
	}

	protected static void store(String excelFileName, Workbook workBook) {
		Integer savingCounterForFile = savingOperationCounters.computeIfAbsent(excelFileName, key -> 0) + 1;
		File file = new File(PersistentStorage.buildWorkingPath() + File.separator + excelFileName);
		savingOperationCounters.put(excelFileName, savingCounterForFile);
		if (savingCounterForFile % 50 == 0) {
			backup(file);
		}
		try (OutputStream destFileOutputStream = new FileOutputStream(file)){
			BaseFormulaEvaluator.evaluateAllFormulaCells(workBook);
			workBook.write(destFileOutputStream);
		} catch (IOException e) {
			Throwables.INSTANCE.throwException(e);
		}
	}

	protected static void backup(File file) {
		ResourceUtils.INSTANCE.backup(
			file,
			file.getParentFile().getAbsolutePath()
		);
		List<File> backupFiles = ResourceUtils.INSTANCE.findOrdered("report - ", "xlsx", file.getParentFile().getAbsolutePath());
		if (backupFiles.size() > 4) {
			Iterator<File> backupFileIterator = backupFiles.iterator();
			while (backupFiles.size() > 4) {
				File backupFile = backupFileIterator.next();
				backupFile.delete();
				backupFileIterator.remove();
			}
		}
	}
}
