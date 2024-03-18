package org.rg.game.lottery.application;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.FillPatternType;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.util.CellReference;
import org.apache.poi.xssf.usermodel.XSSFFont;
import org.apache.poi.xssf.usermodel.XSSFFormulaEvaluator;
import org.apache.poi.xssf.usermodel.XSSFRichTextString;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.rg.game.core.FirestoreWrapper;
import org.rg.game.core.LogUtils;
import org.rg.game.core.MathUtils;
import org.rg.game.core.ResourceUtils;
import org.rg.game.core.TimeUtils;
import org.rg.game.lottery.engine.Premium;
import org.rg.game.lottery.engine.SEComboHandler;
import org.rg.game.lottery.engine.SELotteryMatrixGeneratorEngine;
import org.rg.game.lottery.engine.SEStats;

public class SEMassiveVerifierAndQualityChecker extends Shared {

	public static void main(String[] args) throws IOException {
		check(
			forDate(
				System.getenv().getOrDefault(
					"startDate", "14/02/2023"
				), System.getenv().getOrDefault(
					"endDate", "next+0*1"
				),
				false
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
		//dates.add(new AbstractMap.SimpleEntry(LocalDate.parse("2023-04-24"), false));
		return dates;
	}

	private static void check(List<Map.Entry<LocalDate, Object>>... dateGroupsList) throws IOException {
		Map<String, Map<Number,List<List<Integer>>>> historyData = new LinkedHashMap<>();
		Map<Number, List<List<Integer>>> globalData = new LinkedHashMap<>();
		Map<Integer,Map<String, Map<Number, Integer>>> dataForTime = new LinkedHashMap<>();
		LocalDateTime backupTime = TimeUtils.now();
		for (List<Map.Entry<LocalDate, Object>> dateGroup: dateGroupsList) {
			for (Map.Entry<LocalDate, Object> dateInfo : dateGroup) {
				String extractionDate = TimeUtils.defaultLocalDateFormat.format(dateInfo.getKey());
				String extractionYear = extractionDate.split("\\/")[2];
				String extractionMonth = getMonth(extractionDate);
				String extractionDay = extractionDate.split("\\/")[0];
				File mainFile = getSystemsFile(extractionYear);
				ResourceUtils.INSTANCE.backup(
					backupTime,
					mainFile,
					mainFile.getParentFile().getAbsolutePath() + File.separator + "Backup sistemi"
				);
				List<List<Integer>> system = new ArrayList<>();
				try (InputStream srcFileInputStream = new FileInputStream(mainFile);
					Workbook workbook = new XSSFWorkbook(srcFileInputStream);
				) {
					XSSFFont boldFont = (XSSFFont) workbook.createFont();
					boldFont.setBold(true);
					XSSFFont boldItalicFont = (XSSFFont) workbook.createFont();
					boldItalicFont.setBold(true);
					boldItalicFont.setItalic(true);
					XSSFFont boldHighLightedFont = (XSSFFont) workbook.createFont();
					boldHighLightedFont.setBold(true);
					boldHighLightedFont.setColor(IndexedColors.ORANGE.getIndex());
					XSSFFont boldHighLightedItalicFont = (XSSFFont) workbook.createFont();
					boldHighLightedItalicFont.setBold(true);
					boldHighLightedItalicFont.setItalic(true);
					boldHighLightedItalicFont.setColor(IndexedColors.ORANGE.getIndex());
					CellStyle boldAndCeneteredCellStyle = workbook.createCellStyle();
					boldAndCeneteredCellStyle.setFont(boldFont);
					boldAndCeneteredCellStyle.setAlignment(HorizontalAlignment.CENTER);
					boldAndCeneteredCellStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
					boldAndCeneteredCellStyle.setFillForegroundColor(IndexedColors.ORANGE.getIndex());
					CellStyle boldItalicAndCeneteredCellStyle = workbook.createCellStyle();
					boldItalicAndCeneteredCellStyle.setFont(boldItalicFont);
					boldItalicAndCeneteredCellStyle.setAlignment(HorizontalAlignment.CENTER);
					boldItalicAndCeneteredCellStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
					boldItalicAndCeneteredCellStyle.setFillForegroundColor(IndexedColors.ORANGE.getIndex());
					CellStyle normalAndCeneteredCellStyle = workbook.createCellStyle();
					normalAndCeneteredCellStyle.setAlignment(HorizontalAlignment.CENTER);
					Sheet sheet = workbook.getSheet(extractionMonth);
					if (sheet == null) {
						LogUtils.INSTANCE.warn("Nessun foglio da verificare per il mese " + extractionMonth);
						continue;
					}
					int offset = getCellIndex(sheet, extractionDay);
					if (offset < 0) {
						LogUtils.INSTANCE.warn("Nessuna combinazione da verificare per la data " + extractionDate + "\n");
						continue;
					}
					Iterator<Row> rowIterator = sheet.rowIterator();
					rowIterator.next();
					List<Integer> winningComboWithJollyAndSuperstar = getSEStats().getWinningComboWithJollyAndSuperstarOf(dateInfo.getKey());
					List<Integer> winningCombo =
						winningComboWithJollyAndSuperstar != null ? winningComboWithJollyAndSuperstar.subList(0, 6) : null;
					Integer jolly =
						winningComboWithJollyAndSuperstar != null ? winningComboWithJollyAndSuperstar.get(6) : null;
					Cell jollyCell = null;
					while (rowIterator.hasNext()) {
						Row row = rowIterator.next();
						List<Integer> currentCombo = new ArrayList<>();
						Number hit = (int)0;
						for (int i = 0; i < 6; i++) {
							Cell cell = row.getCell(offset + i);
							try {
								Integer currentNumber = Integer.valueOf((int)cell.getNumericCellValue());
								currentCombo.add(currentNumber);
								if (winningCombo != null && winningCombo.contains(currentNumber)) {
									hit = hit.intValue() + 1;
									cell.setCellStyle(boldAndCeneteredCellStyle);
								} else {
									cell.setCellStyle(normalAndCeneteredCellStyle);
								}
								if (jolly != null && jolly.compareTo(currentNumber) == 0) {
									jollyCell = cell;
								}
							} catch (NullPointerException exc) {
								if (cell == null) {
									break;
								}
								throw exc;
							} catch (IllegalStateException exc) {
								if (cell.getStringCellValue().equals("Risultato estrazione")) {
									break;
								}
								throw exc;
							}
						}
						if (hit.intValue() == Premium.TYPE_FIVE.intValue() && jollyCell != null) {
							jollyCell.setCellStyle(boldItalicAndCeneteredCellStyle);
						}
						if (currentCombo.isEmpty() || currentCombo.get(0) == 0) {
							break;
						}
						system.add(currentCombo);
					}
					rowIterator = sheet.rowIterator();
					sheet.setColumnWidth(offset + 6, 6000);
					rowIterator.next();
					Cell cell = rowIterator.next().getCell(offset + 6);
					XSSFRichTextString results = new XSSFRichTextString();
					LogUtils.INSTANCE.info(
						checkCombo(
							globalData,
							dataForTime,
							dateInfo.getKey(),
							system,
							winningComboWithJollyAndSuperstar,
							results,
							boldFont,
							boldItalicFont,
							boldHighLightedFont,
							boldHighLightedItalicFont
						)
					);
					if (results.getString() != null) {
						results.append("\n");
					}
					checkInFollowingProgressiveHistory(
						//historyData,
						dateInfo.getKey(),
						system,
						getSEStats().getAllWinningCombosWithJollyAndSuperstar(),
						results,
						boldFont
					);
					if (results.getString() != null) {
						results.append("\n\n");
					}
					checkInHistory(
						historyData,
						dateInfo.getKey(),
						system,
						getSEStats().getAllWinningCombosWithJollyAndSuperstar(),
						results,
						boldFont
					);
					if (results.getString() != null) {
						cell.setCellValue(results);
					}
					try (OutputStream destFileOutputStream = new FileOutputStream(mainFile.getAbsolutePath())){
						workbook.write(destFileOutputStream);
					}
				} catch (Throwable exc) {
					LogUtils.INSTANCE.error(exc);
				}
			}
		}
		writeAndPrintData(globalData, dataForTime);
	}

	private static void writeAndPrintData(
		Map<Number, List<List<Integer>>> globalData,
		Map<Integer, Map<String, Map<Number, Integer>>> dataForTime
	) throws IOException {
		LogUtils.INSTANCE.info("\nRisultati per tempo:");
		for (Map.Entry<Integer, Map<String, Map<Number, Integer>>> yearAndDataForMonth : dataForTime.entrySet()) {
			int year = yearAndDataForMonth.getKey();
			Map<String, Map<Number, Integer>> dataForMonth = yearAndDataForMonth.getValue();
			LogUtils.INSTANCE.info("\t" + year + ":");
			File mainFile = getSystemsFile(year);
			try (InputStream srcFileInputStream = new FileInputStream(mainFile);
				Workbook workbook = new XSSFWorkbook(srcFileInputStream);
			) {
				Sheet sheet = getSummarySheet(workbook);
				Iterator<Row> rowIterator = sheet.rowIterator();
				Font normalFont = null;
				Font boldFont = null;
				CellStyle valueStyle = workbook.createCellStyle();
				valueStyle.setFont(normalFont);
				valueStyle.setAlignment(HorizontalAlignment.RIGHT);
				valueStyle.setDataFormat(workbook.createDataFormat().getFormat("#,##0"));
				int rowIndex = 0;
				Row header;
				if (rowIterator.hasNext()) {
					header = rowIterator.next();
				} else {
					boldFont = workbook.createFont();
					boldFont.setBold(true);
					normalFont = workbook.createFont();
					normalFont.setBold(false);
					sheet.createFreezePane(0, 1);
					header = sheet.createRow(rowIndex);
					CellStyle headerStyle = workbook.createCellStyle();
					headerStyle.setFont(boldFont);
					headerStyle.setAlignment(HorizontalAlignment.CENTER);
					headerStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
					headerStyle.setFillForegroundColor(IndexedColors.YELLOW.getIndex());
					Cell monthLabel = header.createCell(0);
					monthLabel.setCellValue("Mese");
					monthLabel.setCellStyle(headerStyle);
					int columnIndex = 1;
					for (String premiumLabel : Premium.allLabelsList()) {
						Cell headerCell = header.createCell(columnIndex++);
						headerCell.setCellStyle(headerStyle);
						headerCell.setCellValue(premiumLabel);
					}
				}
				rowIndex++;
				for (Map.Entry<String, Map<Number, Integer>> monthWinningInfo : dataForMonth.entrySet()) {
					String month = monthWinningInfo.getKey();
					LogUtils.INSTANCE.info("\t\t" + month + ":");
					Map<Number, Integer> winningInfo = monthWinningInfo.getValue();
					Row row = rowIterator.hasNext() ? rowIterator.next() : sheet.createRow(rowIndex);
					Cell labelCell = row.getCell(0);
					if (labelCell == null) {
						labelCell = row.createCell(0);
						labelCell.getCellStyle().setFont(boldFont);
						labelCell.getCellStyle().setAlignment(HorizontalAlignment.LEFT);
					}
					labelCell.setCellValue(month);
					for (Number type : Premium.all().keySet()) {
						Integer counter = winningInfo.getOrDefault(type, 0);
						String label = Premium.toLabel(type);
						int labelIndex = getCellIndex(sheet, label);
						if (counter > 0) {
							LogUtils.INSTANCE.info("\t\t\t" + label + ":" + SEStats.rightAlignedString(MathUtils.INSTANCE.integerFormat.format(counter), 21 - label.length()));
						}
						Cell valueCell = row.getCell(labelIndex);
						if (valueCell == null) {
							valueCell = row.createCell(labelIndex);
						}
						valueCell.removeFormula();
						valueCell.setCellStyle(valueStyle);
						if (counter > 0) {
							valueCell.setCellValue(counter);
						} else {
							valueCell.setCellValue((String)null);
						}
						if (rowIndex == dataForMonth.entrySet().size()) {
							Row summaryRow = sheet.getRow(rowIndex + 1) != null ?
								sheet.getRow(rowIndex + 1) : sheet.createRow(rowIndex + 1);
							if (summaryRow.getCell(0) == null) {
								labelCell = summaryRow.createCell(0);
								labelCell.getCellStyle().setFont(boldFont);
								labelCell.getCellStyle().setAlignment(HorizontalAlignment.LEFT);
								labelCell.setCellValue("Totale");
							}
							valueCell = summaryRow.getCell(labelIndex);
							if (valueCell == null) {
								valueCell = summaryRow.createCell(labelIndex);
							}
							valueCell.setCellStyle(valueStyle);
							String columnName = CellReference.convertNumToColString(labelIndex);
							valueCell.setCellFormula("SUM(" + columnName + "2:"+ columnName + (rowIndex + 1) +")");
						}
					}
					rowIndex++;
				}
				XSSFFormulaEvaluator.evaluateAllFormulaCells(workbook);
				try (OutputStream destFileOutputStream = new FileOutputStream(mainFile.getAbsolutePath())){
					workbook.write(destFileOutputStream);
				}
			} catch (Throwable exc) {
				System.err.println("Unable to process file: " + exc.getMessage());
			}
		};

		LogUtils.INSTANCE.info("\nRisultati globali:");
		globalData.forEach((key, combos) -> {
			String label = Premium.toLabel(key);
			LogUtils.INSTANCE.info("\t" + label + ":" + SEStats.rightAlignedString(MathUtils.INSTANCE.integerFormat.format(combos.size()), 21 - label.length()));
		});
	}

	private static String checkCombo(
		Map<Number,List<List<Integer>>> globalData,
		Map<Integer, Map<String, Map<Number, Integer>>> dataForTime,
		LocalDate extractionDate,
		List<List<Integer>> combosToBeChecked,
		List<Integer> winningComboWithJollyAndSuperstar,
		XSSFRichTextString results,
		XSSFFont boldFont,
		XSSFFont boldItalicFont,
		XSSFFont boldHighLightedFont,
		XSSFFont boldHighLightedItalicFont
	) {
		if (winningComboWithJollyAndSuperstar == null || winningComboWithJollyAndSuperstar.isEmpty()) {
			return "Nessuna estrazione per il concorso del " + TimeUtils.defaultLocalDateFormat.format(extractionDate) + "\n";
		}
		List<Integer> winningCombo = winningComboWithJollyAndSuperstar.subList(0, 6);
		Map<Number,List<List<Integer>>> winningCombos = new TreeMap<>();
		Collection<Integer> hitNumbers = new LinkedHashSet<>();
		Integer jolly = winningComboWithJollyAndSuperstar.get(6);
		for (List<Integer> currentCombo : combosToBeChecked) {
			Number hit = 0;
			for (Integer currentNumber : currentCombo) {
				if (winningCombo.contains(currentNumber)) {
					hitNumbers.add(currentNumber);
					hit = hit.intValue() + 1;
				}
			}
			if (hit.intValue() > 1) {
				if (hit.intValue() == Premium.TYPE_FIVE.intValue()) {
					if (currentCombo.contains(jolly)) {
						hit = Premium.TYPE_FIVE_PLUS;
						hitNumbers.add(jolly);
					}
				}
				winningCombos.computeIfAbsent(hit, ht -> new ArrayList<>()).add(currentCombo);
				Map<Number, Integer> winningCounter = dataForTime.computeIfAbsent(extractionDate.getYear(), year -> new LinkedHashMap<>()).computeIfAbsent(
					getMonth(extractionDate), monthLabel -> new LinkedHashMap<>()
				);
				winningCounter.put(hit, winningCounter.computeIfAbsent(hit, key -> Integer.valueOf(0)) + 1);
				globalData.computeIfAbsent(hit, ht -> new ArrayList<>()).add(currentCombo);
			}
		}
		results.append("Vincente", boldFont);
		results.append(":\n    ");
		printWinningComboWithHitHighLights(winningComboWithJollyAndSuperstar, hitNumbers, results, boldHighLightedFont, boldHighLightedItalicFont);
		results.append("\n");
		results.append("Concorso", boldFont);
		results.append(":\n");
		if (!winningCombos.isEmpty()) {
			printSummaryWinningInfo(results, boldFont, winningCombos);
			//printDetailedWinningInfo(winningComboWithJollyAndSuperstar, results, boldFont, boldItalicFont, winningCombos);
		} else {
			results.append("    nessuna vincita");
		}
		results.append("\n");
		StringBuffer result = new StringBuffer();
		if (!winningCombo.isEmpty()) {
			if (!winningCombos.isEmpty()) {
				result.append("Numeri estratti per il *superenalotto* del " + TimeUtils.defaultLocalDateFormat.format(extractionDate) +": " + SEComboHandler.toWAString(winningComboWithJollyAndSuperstar, ", ", " - ", hitNumbers) + "\n");
				for (Map.Entry<Number, List<List<Integer>>> combos: winningCombos.entrySet()) {
					result.append("\t*Combinazioni con " + Premium.toLabel(combos.getKey()).toLowerCase() + "*:" + "\n");
					for (List<Integer> combo : combos.getValue()) {
						result.append("\t\t" +
							SEComboHandler.toWAString(combo, "\t", " - ", winningComboWithJollyAndSuperstar) + "\n"
						);
					}
				}
			} else {
				result.append("Nessuna vincita per il concorso del " + TimeUtils.defaultLocalDateFormat.format(extractionDate) + "\n");
			}
		}
		return result.toString();
	}

	private static void printWinningComboWithHitHighLights(
		List<Integer> winningComboWithJollyAndSuperstar,
		Collection<Integer> hitNumbers,
		XSSFRichTextString results,
		XSSFFont boldHighLightedFont,
		XSSFFont boldHighLightedItalicFont
	) {
		List<Integer> winningCombo = winningComboWithJollyAndSuperstar.subList(0, 6);
		hitNumbers = new ArrayList<>(hitNumbers);
		boolean jollyHit = hitNumbers.remove(winningComboWithJollyAndSuperstar.get(6));
		Iterator<Integer> winningComboIterator = winningCombo.iterator();
		while (winningComboIterator.hasNext()) {
			Integer winningNumber = winningComboIterator.next();
			if (hitNumbers.contains(winningNumber)) {
				results.append(winningNumber.toString(), boldHighLightedFont);
			} else {
				results.append(winningNumber.toString());
			}
			if (winningComboIterator.hasNext()) {
				results.append(", ");
			}
		}
		Integer jolly = winningComboWithJollyAndSuperstar.get(6);
		results.append(" - ");
		if (hitNumbers.size() == Premium.TYPE_FIVE.intValue() && jollyHit) {
			results.append("" + jolly, boldHighLightedItalicFont);
		} else {
			results.append("" + jolly);
		}
		results.append(
			"\n"
		);
	}

	private static void printDetailedWinningInfo(
		List<Integer> winningComboWithJollyAndSuperstar,
		XSSFRichTextString results,
		XSSFFont boldFont,
		XSSFFont boldItalicFont,
		Map<Number, List<List<Integer>>> winningCombos
	) {
		List<Integer> winningCombo = winningComboWithJollyAndSuperstar.subList(0, 6);
		Integer jolly = winningComboWithJollyAndSuperstar.get(6);
		for (Map.Entry<Number, List<List<Integer>>> combos: winningCombos.entrySet()) {
			results.append("    " + Premium.toLabel(combos.getKey()), boldFont);
			results.append(":" + "\n");
			Iterator<List<Integer>> combosIterator = combos.getValue().iterator();
			while (combosIterator.hasNext()) {
				List<Integer> currentCombo = combosIterator.next();
				results.append("        ");
				Iterator<Integer> winningComboIterator = currentCombo.iterator();
				while (winningComboIterator.hasNext()) {
					Integer number = winningComboIterator.next();
					if (winningCombo.contains(number)) {
						results.append(number.toString(), boldFont);
					} else if (combos.getKey().doubleValue() == Premium.TYPE_FIVE_PLUS.doubleValue() && number.compareTo(jolly) == 0) {
						results.append(number.toString(), boldItalicFont);
					} else {
						results.append(number.toString());
					}
					if (winningComboIterator.hasNext()) {
						results.append(", ");
					}
				}
				if (combosIterator.hasNext()) {
					results.append("\n");
				}
			}
		}
	}

	private static void printSummaryWinningInfo(
		XSSFRichTextString results,
		XSSFFont boldFont,
		Map<Number, List<List<Integer>>> winningCombos
	) {
		Iterator<Map.Entry<Number, List<List<Integer>>>> winningAndCombosIterator = winningCombos.entrySet().iterator();
		while (winningAndCombosIterator.hasNext()) {
			Map.Entry<Number, List<List<Integer>>> combos = winningAndCombosIterator.next();
			results.append("    " + Premium.toLabel(combos.getKey()), boldFont);
			results.append(": " + combos.getValue().size());
			if (winningAndCombosIterator.hasNext()) {
				results.append("\n");
			}
		}
	}

	private static void checkInFollowingProgressiveHistory(
		LocalDate extractionDate,
		List<List<Integer>> system,
		Map<Date, List<Integer>> allWinningCombosWithJollyAndSuperstar,
		XSSFRichTextString results,
		XSSFFont boldFont
	) {
		Map<String, Map<Number,List<List<Integer>>>> historyData = new LinkedHashMap<>();
		Collection<Integer> hitNumbers = new LinkedHashSet<>();
		String extractionDateAsString = TimeUtils.defaultLocalDateFormat.format(extractionDate);
		for (Map.Entry<Date, List<Integer>> winningComboWithJollyAndSuperstarEntry : allWinningCombosWithJollyAndSuperstar.entrySet()) {
			if (TimeUtils.toLocalDate(winningComboWithJollyAndSuperstarEntry.getKey()).compareTo(extractionDate) >= 0) {
				List<Integer> winningComboWithJollyAndSuperstar = winningComboWithJollyAndSuperstarEntry.getValue();
				List<Integer> winningCombo = winningComboWithJollyAndSuperstar.subList(0, 6);
				for (List<Integer> currentCombo : system) {
					Number hit = 0;
					for (Integer currentNumber : currentCombo) {
						if (winningCombo.contains(currentNumber)) {
							hitNumbers.add(currentNumber);
							hit = hit.intValue() +1;
						}
					}
					if (hit.intValue() > 1) {
						if (hit.intValue() == Premium.TYPE_FIVE.intValue()) {
							if (currentCombo.contains(winningComboWithJollyAndSuperstar.get(6))) {
								hit = Premium.TYPE_FIVE_PLUS;
							}
						}
						historyData.computeIfAbsent(extractionDateAsString, key -> new TreeMap<>(MathUtils.INSTANCE.numberComparator))
						.computeIfAbsent(hit, ht -> new ArrayList<>()).add(currentCombo);
					}
				}
			}
		}
		results.append("Storico prg. ant.", boldFont);
		results.append(":\n");
		Map<Number,List<List<Integer>>> systemResultsInHistory = historyData.get(extractionDateAsString);
		if (systemResultsInHistory != null) {
			Iterator<Map.Entry<Number, List<List<Integer>>>> systemResultsInHistoryItr = systemResultsInHistory.entrySet().iterator();
			while (systemResultsInHistoryItr.hasNext()) {
				Map.Entry<Number, List<List<Integer>>> singleHistoryResult = systemResultsInHistoryItr.next();
				String label = Premium.toLabel(singleHistoryResult.getKey());
				results.append("    ");
				results.append(label, boldFont);
				results.append(": " + MathUtils.INSTANCE.integerFormat.format(singleHistoryResult.getValue().size()));
				if (systemResultsInHistoryItr.hasNext()) {
					results.append("\n");
				}
			}
		} else {
			results.append("    nessuna vincita");
		}
	}

	private static void checkInHistory(
		Map<String, Map<Number,List<List<Integer>>>> historyData,
		LocalDate extractionDate,
		List<List<Integer>> system,
		Map<Date, List<Integer>> allWinningCombosWithJollyAndSuperstar,
		XSSFRichTextString results,
		XSSFFont boldFont
	) {
		Collection<Integer> hitNumbers = new LinkedHashSet<>();
		String extractionDateAsString = TimeUtils.defaultLocalDateFormat.format(extractionDate);
		for (List<Integer> winningComboWithJollyAndSuperstar : allWinningCombosWithJollyAndSuperstar.values()) {
			List<Integer> winningCombo = winningComboWithJollyAndSuperstar.subList(0, 6);
			for (List<Integer> currentCombo : system) {
				Number hit = 0;
				for (Integer currentNumber : currentCombo) {
					if (winningCombo.contains(currentNumber)) {
						hitNumbers.add(currentNumber);
						hit = hit.intValue() +1;
					}
				}
				if (hit.intValue() > 1) {
					if (hit.intValue() == Premium.TYPE_FIVE.intValue()) {
						if (currentCombo.contains(winningComboWithJollyAndSuperstar.get(6))) {
							hit = Premium.TYPE_FIVE_PLUS;
						}
					}
					historyData.computeIfAbsent(extractionDateAsString, key -> new TreeMap<>(MathUtils.INSTANCE.numberComparator))
					.computeIfAbsent(hit, ht -> new ArrayList<>()).add(currentCombo);
				}
			}
		}
		//allWinningCombosWithJollyAndSuperstar.entrySet().stream().reduce((first,second) -> second).get().getKey()
		results.append("Storico dal " + sEStatsDefaultDate, boldFont);
		results.append(":\n");
		Map<Number,List<List<Integer>>> systemResultsInHistory = historyData.get(extractionDateAsString);
		if (systemResultsInHistory != null) {
			Iterator<Map.Entry<Number, List<List<Integer>>>> systemResultsInHistoryItr = systemResultsInHistory.entrySet().iterator();
			while (systemResultsInHistoryItr.hasNext()) {
				Map.Entry<Number, List<List<Integer>>> singleHistoryResult = systemResultsInHistoryItr.next();
				String label = Premium.toLabel(singleHistoryResult.getKey());
				results.append("    ");
				results.append(label, boldFont);
				results.append(": " + MathUtils.INSTANCE.integerFormat.format(singleHistoryResult.getValue().size()));
				if (systemResultsInHistoryItr.hasNext()) {
					results.append("\n");
				}
			}
		} else {
			results.append("    nessuna vincita");
		}
	}

}
