package org.rg.game.lottery.application;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.burningwave.Throwables;
import org.rg.game.core.CollectionUtils;
import org.rg.game.core.FirestoreWrapper;
import org.rg.game.core.LogUtils;
import org.rg.game.core.ResourceUtils;
import org.rg.game.core.TimeUtils;
import org.rg.game.lottery.engine.SELotteryMatrixGeneratorEngine;

public class SELotteryComplexSimulator extends SELotterySimpleSimulator {


	static final String GENERATED_FOLDER_NAME = "generated";

	public static void main(String[] args) throws IOException {
		Collection<CompletableFuture<Void>> futures = new CopyOnWriteArrayList<>();
		executeRecursive(SELotteryComplexSimulator::execute, futures);
		LogUtils.INSTANCE.warn("All activities are finished");
		FirestoreWrapper.shutdownDefaultInstance();
	}

	protected static Collection<CompletableFuture<Void>> execute(
		String configFilePrefix,
		Collection<CompletableFuture<Void>> futures
	) {
		String[] configurationFileFolders = ResourceUtils.INSTANCE.pathsFromSystemEnv(
			"working-path.complex-simulations.folder",
			"resources.complex-simulations.folder"
		);
		LogUtils.INSTANCE.info("Set configuration files folder to " + String.join(", ", configurationFileFolders) + "\n");
		try {
			for (Properties config : toConfigurations(
				ResourceUtils.INSTANCE.find(
					configFilePrefix + "-complex-simulation", "properties",
					configurationFileFolders
				),
				"simulation.slave"
			)) {
				if (!CollectionUtils.INSTANCE.retrieveBoolean(config, "simulation.enabled")) {
					continue;
				}
				String[] childrenSimulationsFilters = CollectionUtils.INSTANCE.retrieveValue(config, "simulation.children", "allEnabledInSameFolder").replaceAll("\\s+","").split(",");
				List<File> simpleConfigurationFiles = new ArrayList<>();
				for (String childrenSimulationsFilter : childrenSimulationsFilters) {
					if (childrenSimulationsFilter.equals("allInSameFolder") || childrenSimulationsFilter.equals("allEnabledInSameFolder")) {
						simpleConfigurationFiles.addAll(
							ResourceUtils.INSTANCE.find(
								configFilePrefix + "-simple-simulation", "properties",
								ResourceUtils.INSTANCE.pathsFromSystemEnv(
									"working-path.complex-simulations.folder",
									"resources.complex-simulations.folder"
								)
							)
						);
						if (childrenSimulationsFilter.equals("allEnabledInSameFolder") || childrenSimulationsFilter.equals("allInSameFolder")) {
							Iterator<File> simpleConfigFileIterator = simpleConfigurationFiles.iterator();
							while (simpleConfigFileIterator.hasNext()) {
								Properties simpleConfig = ResourceUtils.INSTANCE.toProperties(simpleConfigFileIterator.next());
								if (childrenSimulationsFilter.equals("allEnabledInSameFolder") && !CollectionUtils.INSTANCE.retrieveBoolean(simpleConfig, "simulation.enabled", false)) {
									simpleConfigFileIterator.remove();
								} else if (childrenSimulationsFilter.equals("allInSameFolder")) {
									simpleConfig.setProperty("simulation.enabled", "true");
								}
							}
						}
					} else {
						simpleConfigurationFiles.add(ResourceUtils.INSTANCE.toFile(CollectionUtils.INSTANCE.retrieveValue(config, "file.parent.absolutePath"), childrenSimulationsFilter));
					}
				}
				if (CollectionUtils.INSTANCE.retrieveValue(config, "simulation.group") == null) {
					config.setProperty("simulation.group", CollectionUtils.INSTANCE.retrieveValue(config, "file.name").replace(".properties", ""));
				}
				List<Properties> simpleConfigurations = ResourceUtils.INSTANCE.toOrderedProperties(simpleConfigurationFiles);
				for (Properties simpleConfiguration : simpleConfigurations) {
					for (String propertyName : config.stringPropertyNames()) {
						if (!(
								propertyName.contains("children") ||
								propertyName.equals("file.name") ||
								propertyName.equals("file.extension") ||
								propertyName.equals("file.parent.absolutePath") ||
								propertyName.equals("simulation.dates")
							)
						) {
							simpleConfiguration.setProperty(propertyName, CollectionUtils.INSTANCE.retrieveValue(config, propertyName));
						} else if (propertyName.equals("simulation.children.async")) {
							simpleConfiguration.setProperty("simulation.async", CollectionUtils.INSTANCE.retrieveValue(config, "simulation.children.async"));
						}
					}
					simpleConfiguration.setProperty("simulation.enabled", "true");
				}
				String extractionDatesExpression = CollectionUtils.INSTANCE.retrieveValue(config, "simulation.children.dates");
				if (extractionDatesExpression != null) {
					List<LocalDate> extractionDates = new ArrayList<>(
						SELotteryMatrixGeneratorEngine.DEFAULT_INSTANCE.computeExtractionDates(extractionDatesExpression)
					);
					if (!extractionDates.isEmpty()) {
						LocalDate nextAfterLatest = removeNextOfLatestExtractionDate(config, extractionDates);
						Supplier<Integer> configurationIndexIterator = indexIterator(config, extractionDates, simpleConfigurations);
						Map<Properties, Set<LocalDate>> extractionDatesForSimpleConfigs = new LinkedHashMap<>();
						for (int i = 0; i < extractionDates.size(); i++) {
							Properties simpleConfiguration = simpleConfigurations.get(configurationIndexIterator.get());
							Set<LocalDate> extractionDatesForConfig=
									extractionDatesForSimpleConfigs.computeIfAbsent(simpleConfiguration, key -> new TreeSet<>());
							extractionDatesForConfig.add(extractionDates.get(i));
						}
						for (Map.Entry<Properties, Set<LocalDate>> extractionDatesForSimpleConfig : extractionDatesForSimpleConfigs.entrySet()) {
							extractionDatesForSimpleConfig.getKey().setProperty(
								"simulation.dates",
								String.join(
									",",
									extractionDatesForSimpleConfig.getValue().stream().map(TimeUtils.defaultLocalDateFormat::format).collect(Collectors.toList())
								)
							);
						}
						prepareAndProcess(futures, SELotteryMatrixGeneratorEngine::new, simpleConfigurations);
						if (nextAfterLatest != null) {
							generateSystem(futures, simpleConfigurations.get(configurationIndexIterator.get()), nextAfterLatest);
						}
						/*backup(
							new File(
								PersistentStorage.buildWorkingPath() + File.separator + retrieveExcelFileName(complexSimulationConfig, "simulation.group")
							),
							CollectionUtils.INSTANCE.retrieveBoolean(complexSimulationConfig, "simulation.slave", "false")
						);*/
					}
				} else {
					extractionDatesExpression = CollectionUtils.INSTANCE.retrieveValue(config, "simulation.dates");
					if (extractionDatesExpression != null) {
						for (Properties simpleConfiguration : simpleConfigurations) {
							simpleConfiguration.setProperty("simulation.dates", extractionDatesExpression);
						}
					}
					Map<LocalDate, List<Properties>> configurationsOfNextAfterLatest = new LinkedHashMap<>();
					for (Properties simpleConfiguration : simpleConfigurations) {
						Collection<LocalDate> extractionDates = new SELotteryMatrixGeneratorEngine().computeExtractionDates(CollectionUtils.INSTANCE.retrieveValue(simpleConfiguration, "simulation.dates"));
						LocalDate nextAfterLatest = removeNextOfLatestExtractionDate(config, extractionDates);
						if (nextAfterLatest != null ) {
							configurationsOfNextAfterLatest.computeIfAbsent(nextAfterLatest, key -> new ArrayList<>())
							.add(simpleConfiguration);
						}
						simpleConfiguration.setProperty(
							"simulation.dates",
							String.join(
								",",
								extractionDates.stream().map(TimeUtils.defaultLocalDateFormat::format).collect(Collectors.toList())
							)
						);
					}
					prepareAndProcess(futures, SELotteryMatrixGeneratorEngine::new, simpleConfigurations);
					for (Map.Entry<LocalDate, List<Properties>> configurationsOfNextAfterLatestEntry : configurationsOfNextAfterLatest.entrySet()) {
						for (Properties simpleConfiguration : configurationsOfNextAfterLatestEntry.getValue()) {
							generateSystem(futures, simpleConfiguration, configurationsOfNextAfterLatestEntry.getKey());
						}
					}
					/*backup(
						new File(
							PersistentStorage.buildWorkingPath() + File.separator + retrieveExcelFileName(complexSimulationConfig, "simulation.group")
						),
						CollectionUtils.INSTANCE.retrieveBoolean(complexSimulationConfig, "simulation.slave", "false")
					);*/
				}
			}
		} catch (IOException exc) {
			Throwables.INSTANCE.throwException(exc);
		}
		return futures;
	}

	protected static void generateSystem(
		Collection<CompletableFuture<Void>> futures,
		Properties configuration,
		LocalDate date
	) {
		Properties nextAfterLatestConfiguration = new Properties();
		nextAfterLatestConfiguration.putAll(configuration);
		nextAfterLatestConfiguration.put("competition", TimeUtils.defaultLocalDateFormat.format(date));
		nextAfterLatestConfiguration.setProperty("storage", "filesystem");
		nextAfterLatestConfiguration.setProperty("overwrite-if-exists", "1");
		String simulationGroup = CollectionUtils.INSTANCE.retrieveValue(nextAfterLatestConfiguration, "simulation.group");
		if (simulationGroup != null) {
			nextAfterLatestConfiguration.setProperty("simulation.group", simulationGroup + File.separator + GENERATED_FOLDER_NAME);
		}
		setGroup(nextAfterLatestConfiguration);
		nextAfterLatestConfiguration.setProperty(
			"group",
			CollectionUtils.INSTANCE.retrieveValue(nextAfterLatestConfiguration, "simulation.group")
		);
		LotteryMatrixGenerator.process(futures, SELotteryMatrixGeneratorEngine::new, nextAfterLatestConfiguration);
	}

	protected static Supplier<Integer> indexIterator(Properties config, List<LocalDate> extractionDates, List<Properties> simpleConfigurations) {
		String simulationsOrder = CollectionUtils.INSTANCE.retrieveValue(config, "simulation.children.order", "sequence");
		if (simulationsOrder.startsWith("random")) {
			IntSupplier stepSupplier = buildStepSupplier(extractionDates, simulationsOrder);
			Supplier<Integer> randomItr = randomIterator(extractionDates, 0, simpleConfigurations.size())::next;
			AtomicInteger indexWrapper = new AtomicInteger(randomItr.get());
			AtomicInteger latestStepValue = new AtomicInteger(stepSupplier.getAsInt());
			return () -> {
				if (latestStepValue.decrementAndGet() == 0) {
					latestStepValue.set(stepSupplier.getAsInt());
					return indexWrapper.getAndSet(randomItr.get());
				}
				return indexWrapper.get();
			};
		} else if (simulationsOrder.startsWith("sequence")) {
			IntSupplier stepSupplier = buildStepSupplier(extractionDates, simulationsOrder);
			AtomicInteger indexWrapper = new AtomicInteger(0);
			AtomicInteger latestStepValue = new AtomicInteger(stepSupplier.getAsInt());
			return () -> {
				if (indexWrapper.get() == simpleConfigurations.size()) {
					indexWrapper.set(0);
				}
				if (latestStepValue.decrementAndGet() == 0) {
					latestStepValue.set(stepSupplier.getAsInt());
					return indexWrapper.getAndIncrement();
				} else {
					return indexWrapper.get();
				}
			};
		}
		throw new IllegalArgumentException("Unvalid simulation.order parameter value");
	}

	protected static IntSupplier buildStepSupplier(List<LocalDate> extractionDates, String rawOptions) {
		String[] options = rawOptions.split(":");
		return options.length > 1?
			options[1].contains("random") ?
				boundedRandomizer(extractionDates, options[1].split("random")[1].replaceAll("\\s+","").split("->")) :
				() -> Integer.parseInt(options[1]) :
					() -> 1;
	}

	private static IntSupplier boundedRandomizer(List<LocalDate> extractionDates, String[] minAndMax) {
		if (minAndMax.length != 2) {
			minAndMax = new String[] {"1", minAndMax[0]};
		}
		Random random = new Random(allTimeStats.getSeedData(extractionDates.stream().findFirst().orElseGet(() -> null)).getValue());
		Iterator<Integer> randomIterator = random.ints(Integer.parseInt(minAndMax[0]), Integer.parseInt(minAndMax[1]) + 1).boxed().iterator();
		return randomIterator::next;
	}

	protected static Iterator<Integer> randomIterator(List<LocalDate> extractionDates, int minValue, int maxValue) {
		Random random = new Random(allTimeStats.getSeedData(extractionDates.stream().findFirst().orElseGet(() -> null)).getValue());
		return random.ints(minValue, maxValue).boxed().iterator();
	}

}
