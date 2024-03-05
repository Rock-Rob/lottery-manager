package org.rg.game.lottery.application;

import java.io.File;
import java.time.LocalDate;
import java.time.format.TextStyle;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.poi.openxml4j.util.ZipSecureFile;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.FillPatternType;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.util.IOUtils;
import org.apache.poi.xssf.usermodel.XSSFFont;
import org.burningwave.Throwables;
import org.rg.game.core.CollectionUtils;
import org.rg.game.core.TimeUtils;
import org.rg.game.lottery.engine.PersistentStorage;
import org.rg.game.lottery.engine.SELotteryMatrixGeneratorEngine;
import org.rg.game.lottery.engine.SEStats;

class Shared {

	static {
		ZipSecureFile.setMinInflateRatio(0);
		IOUtils.setByteArrayMaxOverride(1000000000);
	}

	static String sEStatsDefaultDate = System.getenv("competition.archive.start-date") != null ?
		System.getenv("competition.archive.start-date"):
		SELotteryMatrixGeneratorEngine.DEFAULT_INSTANCE.getExtractionArchiveStartDate();

	static String capitalizeFirstCharacter(String value) {
		return Character.toString(value.charAt(0)).toUpperCase()
		+ value.substring(1, value.length());
	}

	static LocalDate convert(String dateAsString) {
		if (dateAsString.equals("today")) {
			return TimeUtils.today();
		}
		return CollectionUtils.INSTANCE.getLastElement(
			SELotteryMatrixGeneratorEngine.DEFAULT_INSTANCE.computeExtractionDates(dateAsString)
		);
	}

	static String getMonth(String date) {
		return getMonth(LocalDate.parse(date, TimeUtils.defaultLocalDateFormat));
	}

	static String getMonth(LocalDate date) {
		return capitalizeFirstCharacter(
			date.getMonth().getDisplayName(TextStyle.FULL, Locale.ITALY)
		);
	}

	static int getCellIndex(Sheet sheet, Date localDate) {
		return getCellIndex(sheet, 0, localDate);
	}

	static int getCellIndex(Sheet sheet, int headerIndex, Date date) {
		Row header = sheet.getRow(headerIndex);
		Iterator<Cell> cellIterator = header.cellIterator();
		while (cellIterator.hasNext()) {
			Cell cell = cellIterator.next();
			if (CellType.NUMERIC.equals(cell.getCellType()) && date.compareTo(cell.getDateCellValue()) == 0 ) {
				return cell.getColumnIndex();
			}
		}
		return -1;
	}

	static int getCellIndex(Sheet sheet, String label) {
		return getCellIndex(sheet, 0, label);
	}

	static int getCellIndex(Sheet sheet, int headerIndex, String label) {
		Row header = sheet.getRow(headerIndex);
		Iterator<Cell> cellIterator = header.cellIterator();
		while (cellIterator.hasNext()) {
			Cell cell = cellIterator.next();
			if (CellType.STRING.equals(cell.getCellType()) && label.equals(cell.getStringCellValue())) {
				return cell.getColumnIndex();
			}
		}
		return -1;
	}

	static File getSystemsFile(Integer year) {
		return getSystemsFile(year.toString());
	}

	static File getSystemsFile(String extractionYear) {
		String suffix = System.getenv("file-to-be-processed-suffix");
		File file = new File(PersistentStorage.buildWorkingPath() +
			File.separator + "[SE]["+ extractionYear +"] - " + (suffix != null ? suffix : "Sistemi") +".xlsx");
		//LogUtils.INSTANCE.logInfo("Processing file " + file.getName());
		return file;
	}

	static Sheet getSummarySheet(Workbook workbook) {
		Sheet sheet = workbook.getSheet("Riepilogo");
		if (sheet == null) {
			sheet = workbook.createSheet("Riepilogo");
			workbook.setSheetOrder("Riepilogo", 0);
		}
		return sheet;
	}

	static SEStats getSEStats() {
		return SEStats.get(Shared.sEStatsDefaultDate, TimeUtils.getDefaultDateFormat().format(new Date()));
	}

	static SEStats getSEStatsForLatestExtractionDate() {
		return SEStats.get(Shared.sEStatsDefaultDate, TimeUtils.getDefaultDateFormat().format(getSEStats().getLatestExtractionDate()));
	}

	static SEStats getSEAllStats() {
		return SEStats.get(SEStats.FIRST_EXTRACTION_DATE_AS_STRING, TimeUtils.getDefaultDateFormat().format(new Date()));
	}

	static String rightAlignedString(String value, int emptySpacesCount) {
		return String.format("%" + emptySpacesCount + "s", value);
	}

	static Cell toHighlightedBoldedCell(Workbook workbook, Cell cell, IndexedColors color) {
		CellStyle highLightedBoldedNumberCellStyle = workbook.createCellStyle();
		highLightedBoldedNumberCellStyle.cloneStyleFrom(cell.getCellStyle());
		XSSFFont boldFont = (XSSFFont) workbook.createFont();
		boldFont.setBold(true);
		highLightedBoldedNumberCellStyle.setFont(boldFont);
		highLightedBoldedNumberCellStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
		highLightedBoldedNumberCellStyle.setFillForegroundColor(color.getIndex());
		cell.setCellStyle(highLightedBoldedNumberCellStyle);
		return cell;
	}

	static void removeRows(
		List<Row> rows,
		Comparator<Row> rowsComparator
	) {
		removeRows(
			rows,
			rowsComparator,
			row -> rowIndex -> exception -> {
				if (exception != null) {
					Throwables.INSTANCE.throwException(exception);
				}
			}
		);
	}


	static void removeRows(
		List<Row> rows,
		Comparator<Row> rowsComparator,
		Function<Row, Function<Integer, Consumer<Throwable>>> postProcessing
	) {
		for (Row row : sortRows(rows, rowsComparator, true)) {
			Integer rowIndex = null;
			try {
				rowIndex = row.getRowNum();
				removeRow(row);
				postProcessing.apply(row).apply(rowIndex).accept(null);
			} catch (Throwable exc) {
				postProcessing.apply(row).apply(rowIndex).accept(exc);
			}
		}
	}

	static List<Row> sortRows(List<Row> sortedRows, Comparator<Row> rowsComparator, boolean reversed) {
		int transformer = reversed ? -1 : 1;
		Collections.sort(sortedRows, (rowOne, rowTwo) -> {
			return rowsComparator.compare(rowOne, rowTwo) * transformer;
		});
		return sortedRows;
	}

	private static void removeRow(Row row) {
		Sheet sheet = row.getSheet();
		if (row != null) {
			int rowIndex = row.getRowNum();
			sheet.removeRow(row);
			int lastRowNum = sheet.getLastRowNum();
			if (rowIndex >= 0 && rowIndex < lastRowNum) {
				sheet.shiftRows(rowIndex + 1, lastRowNum, -1);
		    }
		}
	}

	/*
	public static void main(String[] args) {
		List<Integer> ourNumbers = Arrays.asList(
			1,2,3,4,5,7,8,9,
			10,11,12,13,14,16,17,19,
			20,21,23,24,25,27,28,29,
			32,33,35,
			40,47,49,
			51,52,55,
			64,68,69,
			75,77,79,
			80,83,84,85,86,88,90
		);
		//LogUtils.INSTANCE.logInfo(ComboHandler.sizeOf(ComboHandler.sizeOf(ourNumbers.size(), 6), 34));
		int count = 0;
		List<List<Integer>> system = new ArrayList<>();
		int bound = 4;
		for (List<Integer> winningCombo : getSEStats().getAllWinningCombos().values()) {
			int hit = 0;
			winningCombo = new ArrayList<>(winningCombo);
			Iterator<Integer> winningComboItr = winningCombo.iterator();
			while (winningComboItr.hasNext()) {
				Integer winningNumber = winningComboItr.next();
				if (ourNumbers.contains(winningNumber)) {
					++hit;
				} else {
					winningComboItr.remove();
				}
			}
			if (hit == bound) {
				system.add(winningCombo);
				LogUtils.INSTANCE.info(ComboHandler.toString(winningCombo, "\t"));
			}
		}
		SELotteryMatrixGeneratorEngine engine = new SELotteryMatrixGeneratorEngine();
		engine.extractionDate = TimeUtils.today();
		engine.adjustSeed();
		List<String> inClauses = new ArrayList<>();
		for (List<Integer> winningCombo : system) {
			inClauses.add("in " + ComboHandler.toString(winningCombo, ",") + ":" + bound + "," + 6);
		}
		LogUtils.INSTANCE.info("(" + String.join("|", inClauses) + ")");
	}*/

}
