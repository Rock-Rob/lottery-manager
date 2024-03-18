package org.rg.game.lottery.application;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.rg.game.core.FirestoreWrapper;
import org.rg.game.core.LogUtils;
import org.rg.game.core.TimeUtils;
import org.rg.game.lottery.engine.SELotteryMatrixGeneratorEngine;

public class SEQualityCheckerForExcel extends Shared {

	public static void main(String[] args) throws IOException {
		check(
			forDate(
				System.getenv().getOrDefault(
					"startDate", "14/02/2023"
				), System.getenv().getOrDefault(
					"endDate", "next+0"
				),
				true
			)
		);
		FirestoreWrapper.shutdownDefaultInstance();
	}

	static List<Map.Entry<LocalDate, Object>> forDate(
		String startDateAsString,
		String endDateAsString,
		Boolean printReportDetail
	) {
		LocalDate startDate = convert(startDateAsString);
		LocalDate endDate =  convert(endDateAsString);
		List<Map.Entry<LocalDate, Object>> dates = new ArrayList<>();
		while (startDate.compareTo(endDate) <= 0) {
			startDate = SELotteryMatrixGeneratorEngine.DEFAULT_INSTANCE.computeNextExtractionDate(startDate, false);
			dates.add(new AbstractMap.SimpleEntry<LocalDate, Object>(startDate, printReportDetail));
			startDate =  SELotteryMatrixGeneratorEngine.DEFAULT_INSTANCE.computeNextExtractionDate(startDate.plus(1, ChronoUnit.DAYS), false);
		}
		return dates;
	}

	private static void check(List<Map.Entry<LocalDate, Object>>... dateGroupsList) throws IOException {
		for (List<Map.Entry<LocalDate, Object>> dateGroup: dateGroupsList) {
			for (Map.Entry<LocalDate, Object> dateInfo : dateGroup) {
				String extractionDate = TimeUtils.defaultLocalDateFormat.format(dateInfo.getKey());
				String extractionYear = extractionDate.split("\\/")[2];
				String extractionMonth = getMonth(extractionDate);
				String extractionDay = extractionDate.split("\\/")[0];
				File mainFile =
					getSystemsFile(extractionYear);
				List<List<Integer>> system = new ArrayList<>();
				try (InputStream srcFileInputStream = new FileInputStream(mainFile); Workbook workbook = new XSSFWorkbook(srcFileInputStream);) {
					Sheet sheet = workbook.getSheet(extractionMonth);
					if (sheet == null) {
						LogUtils.INSTANCE.warn("No sheet named '" + extractionMonth + "' to test for date " + extractionDate);
						continue;
					}
					int offset = getCellIndex(sheet, extractionDay);
					if (offset < 0) {
						LogUtils.INSTANCE.warn("No combination to test for date " + extractionDate);
						continue;
					}
					Iterator<Row> rowIterator = sheet.rowIterator();
					rowIterator.next();
					while (rowIterator.hasNext()) {
						Row row = rowIterator.next();
						List<Integer> currentCombo = new ArrayList<>();
						for (int i = 0; i < 6; i++) {
							Cell cell = row.getCell(offset + i);
							if (cell == null) {
								break;
							}
							Integer currentNumber = Integer.valueOf((int)cell.getNumericCellValue());
							currentCombo.add(currentNumber);
						}
						if (currentCombo.isEmpty() || currentCombo.get(0) == 0) {
							break;
						}
						system.add(currentCombo);
					}
					LogUtils.INSTANCE.info("\nAnalisi del sistema del " + extractionDate + ":" );
					Map<String, Object> report = getSEStats().checkQuality(system::iterator);
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

}
