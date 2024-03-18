package org.rg.game.lottery.application;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.rg.game.core.FirestoreWrapper;
import org.rg.game.core.LogUtils;
import org.rg.game.lottery.engine.PersistentStorage;
import org.rg.game.lottery.engine.Storage;

public class SEQualityChecker extends Shared {

	public static void main(String[] args) throws IOException {
		Map<String, Boolean> systemToBeChecked = new LinkedHashMap<>();
		//systemToBeChecked.put("Simulazioni/smallest-abs-rec-45\\[2014-12-06][6][34]se-simple-simulation[1a]smallest-absence-record-45.txt", true);
		//systemToBeChecked.put("Simulazioni/smallest-abs-rec-45\\[2021-05-06][6][34]se-simple-simulation[1a]smallest-absence-record-45.txt", true);
		systemToBeChecked.put("Simulazioni/our-way-of-complex-playing/data\\[2011-12-15][6][34]se-simple-simulation[1a].txt", true);

		check(
			systemToBeChecked
		);
		FirestoreWrapper.shutdownDefaultInstance();
	}

	private static void check(Map<String, Boolean> dateInfos) throws IOException {
		for (Map.Entry<String, Boolean> dateInfo : dateInfos.entrySet()) {
			try {
				String[] dataInfoSplitted = dateInfo.getKey().split("\\\\");
				Storage storage = PersistentStorage.restore(dataInfoSplitted[0], dataInfoSplitted[dataInfoSplitted.length - 1]);
				storage.printAll();
				Map<String, Object> report = getSEStats().checkQuality(storage::iterator);
				if ((boolean)dateInfo.getValue()) {
					LogUtils.INSTANCE.info("\t" + ((String)report.get("report.detail")).replace("\n", "\n\t"));
				}
				LogUtils.INSTANCE.info("\t" + ((String)report.get("report.summary")).replace("\n", "\n\t"));
			} catch (Throwable exc) {
				LogUtils.INSTANCE.error(exc);
			}
		}
	}
}
