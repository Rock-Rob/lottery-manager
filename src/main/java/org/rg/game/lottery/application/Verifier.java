package org.rg.game.lottery.application;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.rg.game.core.FirestoreWrapper;
import org.rg.game.core.LogUtils;
import org.rg.game.core.MathUtils;
import org.rg.game.core.TimeUtils;
import org.rg.game.lottery.engine.ComboHandler;
import org.rg.game.lottery.engine.Premium;
import org.rg.game.lottery.engine.SEComboHandler;
import org.rg.game.lottery.engine.SEStats;

import com.fasterxml.jackson.databind.ObjectMapper;

public class Verifier {

	public static void main(String[] args) throws IOException {
		List<Integer> winningNumbers =
			args.length > 0 ?
				Arrays.stream(args.length > 1 ? args : args[0].replace(" ", "").split(","))
				.map(Integer::valueOf).collect(Collectors.toList())
			: null;
		check(System.getenv().get("competitionName"), null, winningNumbers);
		FirestoreWrapper.shutdownDefaultInstance();
	}

	private static void check(String competionName, String extractionDate, List<Integer> winningComboWithJollyAndSuperstar) throws IOException {
		if (winningComboWithJollyAndSuperstar == null && competionName.equals("Superenalotto")) {
			URL url = new URL("https://www.gntn-pgd.it/gntn-info-web/rest/gioco/superenalotto/estrazioni/ultimoconcorso");
			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			connection.setRequestProperty("accept", "application/json");
			InputStream responseStream = connection.getInputStream();
			Map<String,Object> data = new ObjectMapper().readValue(responseStream, Map.class);
			extractionDate = TimeUtils.getDefaultDateFormat().format(new Date((Long)data.get("dataEstrazione")));
			winningComboWithJollyAndSuperstar = ((List<String>)((Map<String,Object>)data.get("combinazioneVincente")).get("estratti"))
				.stream().map(Integer::valueOf).collect(Collectors.toList());
			winningComboWithJollyAndSuperstar.add(Integer.valueOf(((String)((Map<String,Object>)data.get("combinazioneVincente")).get("numeroJolly"))));
			Optional.ofNullable(((Map<String,Object>)data.get("combinazioneVincente")).get("superstar"))
			.map(String.class::cast).map(Integer::valueOf).map(winningComboWithJollyAndSuperstar::add);
			connection.disconnect();
		}
		if (extractionDate == null) {
			LocalDate startDate = TimeUtils.today();
			if (competionName.equals("Superenalotto")) {
				while (!SEStats.EXTRACTION_DAYS.contains(startDate.getDayOfWeek())) {
					startDate = startDate.minus(1, ChronoUnit.DAYS);
				}
			} else if (competionName.equals("Million Day")) {
				if (TimeUtils.now().compareTo(TimeUtils.today().atTime(20, 30)) < 0) {
					startDate = startDate.minus(1, ChronoUnit.DAYS);
				}
			}
			extractionDate = TimeUtils.getDefaultDateFormat().format(startDate);
		}
		String extractionYear = extractionDate.split("\\/")[2];
		String extractionMonth = Shared.getMonth(extractionDate);
		String extractionDay = extractionDate.split("\\/")[0];
		File mainFile = Shared.getSystemsFile(extractionYear);
		try (InputStream srcFileInputStream = new FileInputStream(mainFile); Workbook workbook = new XSSFWorkbook(srcFileInputStream);) {
			Sheet sheet = workbook.getSheet(extractionMonth);
			int offset = Shared.getCellIndex(sheet, extractionDay);
			if (offset < 0) {
				LogUtils.INSTANCE.warn("No combination to test for date " + extractionDate);
				return;
			}
			Iterator<Row> rowIterator = sheet.rowIterator();
			rowIterator.next();
			Map<Number,List<List<Integer>>> winningCombos = new TreeMap<>(MathUtils.INSTANCE.numberComparator);
			Collection<Integer> hitNumbers = new LinkedHashSet<>();
			int comboCount = 1;
			List<Integer> winningCombo = winningComboWithJollyAndSuperstar.subList(0, 6);
			Integer jolly = winningComboWithJollyAndSuperstar.get(6);
			while (rowIterator.hasNext()) {
				Row row = rowIterator.next();
				Number hit = 0;
				List<Integer> currentCombo = new ArrayList<>();
				for (int i = 0; i < winningCombo.size(); i++) {
					Cell cell = row.getCell(offset + i);
					if (cell == null) {
						break;
					}
					Integer currentNumber = Integer.valueOf((int)cell.getNumericCellValue());
					currentCombo.add(currentNumber);
					if (winningCombo.contains(currentNumber)) {
						hitNumbers.add(currentNumber);
						hit = hit.intValue() + 1;
					}
				}
				if (currentCombo.isEmpty()) {
					break;
				}
				if (hit.intValue() > 1) {
					if (hit.intValue() == Premium.TYPE_FIVE.intValue()) {
						if (currentCombo.contains(jolly)) {
							hit = Premium.TYPE_FIVE_PLUS;
							hitNumbers.add(jolly);
						}
					}
					winningCombos.computeIfAbsent(hit, ht -> new ArrayList<>()).add(currentCombo);
					LogUtils.INSTANCE.info(comboCount + ") " + ComboHandler.toString(currentCombo, "\t"));
				}
				comboCount++;
			}
			if (!winningComboWithJollyAndSuperstar.isEmpty()) {
				LogUtils.INSTANCE.info("\n\nNumeri estratti per il *" + competionName + "* del " + extractionDate +": " + SEComboHandler.toWAString(winningComboWithJollyAndSuperstar, ", ", " - ", hitNumbers));
				if (!winningCombos.isEmpty()) {
					for (Map.Entry<Number, List<List<Integer>>> combos: winningCombos.entrySet()) {
						LogUtils.INSTANCE.info("\t*Combinazioni con " + Premium.toLabel(combos.getKey()).toLowerCase() + "*:");
						for (List<Integer> combo : combos.getValue()) {
							LogUtils.INSTANCE.info("\t\t" +
								SEComboHandler.toWAString(combo, "\t", "\t\t", winningComboWithJollyAndSuperstar)
							);
						}
					}
				} else {
					LogUtils.INSTANCE.info("Nessuna vincita");
				}
				LogUtils.INSTANCE.info();
			}
		} catch (Throwable exc) {
			LogUtils.INSTANCE.error(exc);
		}
	}

}
