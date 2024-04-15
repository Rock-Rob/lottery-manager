package org.rg.game.lottery.application;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import org.rg.game.core.CollectionUtils;
import org.rg.game.core.FirestoreWrapper;
import org.rg.game.core.LogUtils;
import org.rg.game.core.ResourceUtils;
import org.rg.game.lottery.engine.LotteryMatrixGeneratorAbstEngine;
import org.rg.game.lottery.engine.LotteryMatrixGeneratorAbstEngine.ProcessingContext;
import org.rg.game.lottery.engine.MDLotteryMatrixGeneratorEngine;
import org.rg.game.lottery.engine.SELotteryMatrixGeneratorEngine;



public class LotteryMatrixGenerator extends Shared {

	public static void main(String[] args) throws IOException {
		Collection<CompletableFuture<Void>> futures = new ArrayList<>();
		execute("se", futures);
		execute("md", futures);
		futures.stream().forEach(CompletableFuture::join);
		FirestoreWrapper.shutdownDefaultInstance();
	}

	private static <L extends LotteryMatrixGeneratorAbstEngine> void execute(
		String configFilePrefix,
		Collection<CompletableFuture<Void>> futures
	) throws IOException {
		Supplier<LotteryMatrixGeneratorAbstEngine> engineSupplier =
			configFilePrefix.equals("se") ? SELotteryMatrixGeneratorEngine::new :
				configFilePrefix.equals("md") ? MDLotteryMatrixGeneratorEngine::new : null;
		String[] configurationFileFolders = ResourceUtils.INSTANCE.pathsFromSystemEnv(
			"working-path.generations.folder",
			"resources.generations.folder"
		);
		LogUtils.INSTANCE.info("Set configuration files folder to " + String.join(", ", configurationFileFolders) + "\n");
		List<File> configurationFiles =
			ResourceUtils.INSTANCE.find(
				configFilePrefix + "-matrix-generator", "properties",
				configurationFileFolders
			);
		List<Properties> configurations = new ArrayList<>();
		for (Properties config : ResourceUtils.INSTANCE.toOrderedProperties(configurationFiles)) {
			if (CollectionUtils.INSTANCE.retrieveBoolean(config, "enabled", false)) {
				configurations.add(config);
			}
		}
		for (Properties configuration : configurations) {
			process(futures, engineSupplier, configuration);
		}
	}

	protected static void process(
		Collection<CompletableFuture<Void>> futures,
		Supplier<LotteryMatrixGeneratorAbstEngine> engineSupplier,
		Properties config
	) {
		LogUtils.INSTANCE.info(
			"Processing file '" + CollectionUtils.INSTANCE.retrieveValue(config, "file.name") + "' located in '" + CollectionUtils.INSTANCE.retrieveValue(config, "file.parent.absolutePath") + "'"
		);
		String info = CollectionUtils.INSTANCE.retrieveValue(config, "info");
		if (info != null) {
			LogUtils.INSTANCE.info(info);
		}
		LotteryMatrixGeneratorAbstEngine engine = engineSupplier.get();
		config.setProperty("nameSuffix", CollectionUtils.INSTANCE.retrieveValue(config, "file.name")
			.replace("." + CollectionUtils.INSTANCE.retrieveValue(config, "file.extension"), ""));
		ProcessingContext pC = engine.setup(config, true);
		if (CollectionUtils.INSTANCE.retrieveBoolean(config, "async", false)) {
			futures.add(CompletableFuture.runAsync(() -> pC.getExecutor().apply(null).apply(null)));
		} else {
			pC.getExecutor().apply(null).apply(null);
		}
	}

}
