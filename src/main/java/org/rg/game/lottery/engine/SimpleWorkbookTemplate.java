package org.rg.game.lottery.engine;

import java.io.Closeable;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.poi.common.usermodel.HyperlinkType;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.ConditionalFormattingRule;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.DataFormat;
import org.apache.poi.ss.usermodel.FillPatternType;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.FontFormatting;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.ss.usermodel.Hyperlink;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.PatternFormatting;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.SheetConditionalFormatting;
import org.apache.poi.ss.usermodel.VerticalAlignment;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.ss.util.CellReference;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFFont;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.burningwave.Throwables;

public class SimpleWorkbookTemplate implements Closeable {
	Workbook workbook;
	CellStyle headerStyle;
	CellStyle contentStyle;
	CellStyle hyperLinkStyle;
	Map<String, CellStyle> formattedContentStyle;
	DataFormat dataFormat;
	Map<String, Integer> headersSize;
	boolean autosizeColumnFeatureEnabled;
	String currentSheet;
	Map<String, Integer> currentRow;
	Map<String, Integer> currentCol;

	public SimpleWorkbookTemplate() {
		this(false);
	}

	public SimpleWorkbookTemplate(Workbook workbook) {
		this.workbook = workbook;
		autosizeColumnFeatureEnabled = workbook instanceof HSSFWorkbook;
		dataFormat =  this.workbook.createDataFormat();
		headersSize = new HashMap<>();
		headerStyle = createHeaderStyle(this.workbook);
		contentStyle = createContentStyle(this.workbook);
		hyperLinkStyle = createHyperLinkStyle(this.workbook);
		formattedContentStyle = new HashMap<>();
		currentRow = new HashMap<>();
		currentCol = new HashMap<>();
	}

	public SimpleWorkbookTemplate(boolean largeContent) {
		this(largeContent ? new SXSSFWorkbook() : new HSSFWorkbook());
	}

	private int get(String sheetName, Map<String, Integer> map) {
		Integer value = map.get(sheetName);
		if (value == null) {
			 map.put(sheetName, 0);
			 return 0;
		}
		return value;
	}

	private int reset(String sheetName, Map<String, Integer> map) {
		map.put(sheetName, 0);
		return 0;
	}

	private int getAndIncrement(String sheetName, Map<String, Integer> map) {
		Integer value = map.get(sheetName);
		if (value == null) {
			 map.put(sheetName, 1);
			 return 0;
		}
		map.put(sheetName, value + 1);
		return value;
	}

	protected CellStyle createHeaderStyle(Workbook workbook) {
		return createStyle(workbook, cellStyle -> fontStyle -> {
			fontStyle.setBold(true);
			cellStyle.setAlignment(HorizontalAlignment.CENTER);
			cellStyle.setFillForegroundColor(getHeaderColor());
			cellStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
			cellStyle.setVerticalAlignment(VerticalAlignment.TOP);
			cellStyle.setWrapText(true);
		});
	}

	protected short getHeaderColor() {
		return IndexedColors.LIGHT_GREEN.getIndex();
	}

	protected CellStyle createContentStyle(Workbook workbook) {
		return createStyle(workbook, cellStyle -> fontStyle -> {});
	}

	protected CellStyle createHyperLinkStyle(Workbook workbook) {
		return createStyle(workbook, cellStyle -> fontStyle -> {
			fontStyle.setColor(IndexedColors.BLUE.index);
			fontStyle.setUnderline(XSSFFont.U_SINGLE);
		});
	}

	protected String getDefaultNumberFormat() {
		return "#,##0.00";
	}

	protected String getDefaultDateFormat() {
		return "dd/MM/yyyy";
	}

	public CellStyle getOrCreateStyle(Object... objects) {
		String name = null;
		String pattern = null;
		for (int i = 0; i < objects.length; i++) {
			if ((i == 0 && objects[i] instanceof String) ||
				i > 0 && name == null && objects[i] instanceof String) {
				name = (String)objects[i];
				if (i > 0) {
					pattern = name;
				}
			}
		}
		CellStyle style = formattedContentStyle.get(name);
		if (style == null) {
			style = createStyle(
				workbook,
				null
			);
			formattedContentStyle.put(name, style);
		}
		for (int i = 0; i < objects.length; i++) {
			if (objects[i] instanceof CellStyle) {
				style.cloneStyleFrom((CellStyle)objects[i]);
			} else if (objects[i] instanceof HorizontalAlignment) {
				style.setAlignment((HorizontalAlignment)objects[i]);
			} else if (objects[i] instanceof FillPatternType) {
				style.setFillPattern((FillPatternType) objects[i]);
			} else if (objects[i] instanceof IndexedColors) {
				style.setFillForegroundColor(((IndexedColors) objects[i]).getIndex());
			}
		}
		if (pattern != null) {
			style.setDataFormat(dataFormat.getFormat(pattern));
		}
		return style;
	}

	public CellStyle getOrCreateStyle(Workbook workbook, String pattern) {
		CellStyle style = formattedContentStyle.get(pattern);
		if (style == null) {
			style = createStyle(workbook, cellStyle -> fontStyle -> {
				cellStyle.setDataFormat(dataFormat.getFormat(pattern));
			});
			formattedContentStyle.put(pattern, style);
		}
		return style;
	}

	public void disableAutosizeColumnFeature() {
		this.autosizeColumnFeatureEnabled = false;
	}

	public void enableAutosizeColumnFeature() {
		this.autosizeColumnFeatureEnabled = true;
	}

	public Sheet getOrCreateSheet(String name) {
		Sheet sheet = workbook.getSheet(name);
		if (sheet == null) {
			sheet = workbook.createSheet(name);
			if (autosizeColumnFeatureEnabled && sheet instanceof SXSSFSheet) {
				((SXSSFSheet)sheet).trackAllColumnsForAutoSizing();
			}
		}
		return sheet;
	}

	public Sheet getOrCreateSheet(String name, boolean currentSheet) {
		Sheet sheet = getOrCreateSheet(name);
		if (currentSheet) {
			setCurrentSheet(name);
		}
		return sheet;
	}

	public void setCurrentSheet(String name) {
		this.currentSheet = name;
	}

	public Row addRow() {
		Row row = getOrCreateRow(currentSheet, getAndIncrement(currentSheet, currentRow));
		reset(currentSheet, currentCol);
		return row;
	}

	public Row getCurrentRow() {
		return getOrCreateSheet(currentSheet).getRow(currentRow.computeIfAbsent(currentSheet, key -> 0));
	}

	private Row getOrCreateRow(String sheetName, int rowIdx) {
		Sheet sheet = getOrCreateSheet(sheetName);
		Row row = sheet.getRow(rowIdx);
		if (row == null) {
			row = sheet.createRow(rowIdx);
		}
		return row;
	}

	public Row createHeader(String... titles) {
		return createHeader(true, titles);
	}

	public Row createHeader(boolean blocked, String... titles) {
		return createHeader(blocked, Arrays.asList(titles));
	}

	public Row createHeader(boolean blocked, List<String> titles) {
		List<List<String>> titlesList = new ArrayList<>();
		titlesList.add(titles);
		return createHeader(currentSheet, blocked, titlesList);
	}

	public Row createHeader(String sheetName, boolean blocked, List<List<String>> titles) {
		int maxLenght = 0;
		for (int i = 0; i < titles.size(); i++) {
			int columnIndex = 0;
			for (String title : titles.get(i)) {
				createHeaderCell(sheetName, i, columnIndex++, title);
			}
			maxLenght = Math.max(maxLenght, titles.get(i).size());
		}
		headersSize.put(sheetName, maxLenght);
		if (blocked) {
			getOrCreateSheet(sheetName).createFreezePane(0, titles.size());
		}
		return getOrCreateRow(sheetName, titles.size() - 1);
	}

	public List<Cell> addCell(List<String> values) {
		List<Cell> cells = new ArrayList<>();
		for (int i = 0; i < values.size(); i++) {
			cells.add(createCell(currentSheet, get(currentSheet, currentRow), getAndIncrement(currentSheet, currentCol), values.get(i)));
		}
		return cells;
	}

	public List<Cell> addCell(String... value) {
		if (value == null) {
			value = new String[0];
		}
		List<Cell> cells = new ArrayList<>();
		for (int i = 0; i < value.length; i++) {
			cells.add(createCell(currentSheet, get(currentSheet, currentRow), getAndIncrement(currentSheet, currentCol), value[i]));
		}
		return cells;
	}

	public void addCellOrEmptyCell(String... value) {
		if (value == null) {
			value = new String[0];
		}
		for (int i = 0; i < value.length; i++) {
			createCell(currentSheet, get(currentSheet, currentRow), getAndIncrement(currentSheet, currentCol), value[i] != null ? value[i] : "");
		}
	}

	public Cell createCell(int rowIdx, int colIdx, Number value) {
		return createCell(currentSheet, rowIdx, colIdx, value);
	}

	public Cell createCell(int rowIdx, int colIdx, Date value) {
		return createCell(currentSheet, rowIdx, colIdx, value);
	}

	public Cell createCell(int rowIdx, int colIdx, String value) {
		return createCell(currentSheet, rowIdx, colIdx, value);
	}

	public Cell addCell(Number value) {
		return addCell(value, getDefaultNumberFormat());
	}

	public Cell addCell(Number value, String format) {
		return createCell(currentSheet, get(currentSheet, currentRow), getAndIncrement(currentSheet, currentCol), value, format);
	}

	public Cell addCellOrZeroCell(Number value) {
		return addCellOrZeroCell(value, getDefaultNumberFormat());
	}

	public Cell addCellOrZeroCell(Number value, String format) {
		return addCell(Optional.ofNullable(value).orElseGet(() -> BigDecimal.ZERO), format);
	}

	public Cell createCell(String sheetName, int rowIdx, int colIdx, Number value) {
		return createCell(sheetName, rowIdx, colIdx, value, getDefaultNumberFormat());
	}

	public Cell createCell(String sheetName, int rowIdx, int colIdx, Number value, String format) {
		Row row = getOrCreateRow(sheetName, rowIdx);
		Cell cell = row.createCell(colIdx);
		cell.setCellValue(value.floatValue());
		cell.setCellStyle(getOrCreateStyle(workbook, format));
		return cell;
	}

	public Cell addCell(Date value) {
		return addCell(value, getDefaultDateFormat());
	}

	public Cell addFormulaCell(String value) {
		return createFormulaCell(currentSheet, get(currentSheet, currentRow), getAndIncrement(currentSheet, currentCol), value);
	}

	public Cell addFormulaCell(String value, String format) {
		return createFormulaCell(currentSheet, get(currentSheet, currentRow), getAndIncrement(currentSheet, currentCol), value, format);
	}

	public Cell addCell(Date value, String format) {
		return createCell(currentSheet, get(currentSheet, currentRow), getAndIncrement(currentSheet, currentCol), value, format);
	}

	public Cell createCell(String sheetName, int rowIdx, int colIdx, Date value) {
		return createCell(sheetName, rowIdx, colIdx, value, getDefaultDateFormat());
	}

	public Cell createCell(String sheetName, int rowIdx, int colIdx, Date value, String format) {
		Row row = getOrCreateRow(sheetName, rowIdx);
		Cell cell = row.createCell(colIdx);
		cell.setCellValue(value);
		cell.setCellStyle(getOrCreateStyle(workbook, format));
		return cell;
	}

	public Cell createCell(String sheetName, int rowIdx, int colIdx, String value) {
		Row row = getOrCreateRow(sheetName, rowIdx);
		Cell cell = row.createCell(colIdx);
		cell.setCellValue(value);
		cell.setCellStyle(contentStyle);
		return cell;
	}

	private Cell createFormulaCell(String sheetName, int rowIdx, int colIdx, String value, String format) {
		Row row = getOrCreateRow(currentSheet, rowIdx);
		Cell cell = row.createCell(colIdx);
		cell.setCellFormula(value);
		cell.setCellStyle(getOrCreateStyle(workbook, format));
		return cell;
	}

	public Cell createFormulaCell(String sheetName, int rowIdx, int colIdx, String value) {
		Row row = getOrCreateRow(sheetName, rowIdx);
		Cell cell = row.createCell(colIdx);
		cell.setCellFormula(value);
		cell.setCellStyle(contentStyle);
		return cell;
	}

	public Cell createHeaderCell(String sheetName, int rowIdx, int colIdx, String value) {
		Row row = getOrCreateRow(sheetName, rowIdx);
		Cell cell = row.createCell(colIdx);
		if (!value.startsWith("FORMULA_")) {
			cell.setCellValue(value);
		} else {
			cell.setCellFormula(value.replace("FORMULA_", ""));
		}
		cell.setCellStyle(headerStyle);
		return cell;
	}

	CellStyle createStyle(Workbook workbook, Function<CellStyle, Consumer<Font>> styleSetter) {
		CellStyle cellStyle = workbook.createCellStyle();
		Font fontStyle = workbook.createFont();
		cellStyle.setFont(fontStyle);
		if (styleSetter != null) {
			styleSetter.apply(cellStyle).accept(fontStyle);
		}
		return cellStyle;
	}

	public Workbook getWorkbook() {
		return this.workbook;
	}

	public void autosizeColumn() {
		if (autosizeColumnFeatureEnabled) {
			for (Map.Entry<String, Integer> entry : headersSize.entrySet()) {
				int columnIndex = entry.getValue();
				Sheet sheet = getOrCreateSheet(entry.getKey());
				for (int i = 0; i < columnIndex; i++) {
					sheet.autoSizeColumn(i);
				}
			}
		}
	}

	public void setLinkForCell(HyperlinkType type, Cell cell, String address) {
		setLinkForCell(this.workbook, type, cell, hyperLinkStyle, address);
	}

	public static void setLinkForCell(Workbook workbook, HyperlinkType type, Cell cell, CellStyle cellStyle, String address) {
		Hyperlink hyperLink = getCreationHelper(workbook).createHyperlink(type);
		try {
			hyperLink.setAddress(URLEncoder.encode(address, "UTF-8").replace("+", "%20"));
		} catch (UnsupportedEncodingException exc) {
			Throwables.INSTANCE.throwException(exc);
		}
		cell.setHyperlink(hyperLink);
		cell.setCellStyle(cellStyle);
	}

	public CreationHelper getCreationHelper() {
		return getCreationHelper(this.workbook);
	}

	public static CreationHelper getCreationHelper(Workbook workbook) {
		if (workbook instanceof SXSSFWorkbook) {
			return ((SXSSFWorkbook)workbook).getCreationHelper();
		} else if (workbook instanceof HSSFWorkbook) {
			return ((HSSFWorkbook)workbook).getCreationHelper();
		} else if (workbook instanceof XSSFWorkbook) {
			return ((XSSFWorkbook)workbook).getCreationHelper();
		}
		return null;
	}

	public void setAutoFilter() {
		setAutoFilter(0, currentRow.size() - 1, 0, headersSize.get(currentSheet) - 1);
	}

	public void setAutoFilter(int firstRow, int lastRow, int firstCol, int lastCol) {
		workbook.getSheet(currentSheet).setAutoFilter(
			new CellRangeAddress(
				firstRow, lastRow, firstCol, lastCol
			)
		);
	}

	public Sheet addSheetConditionalFormatting(int column, IndexedColors color, byte comparisonOperator, String... comparisonValue) {
		return addSheetConditionalFormatting(
			new int[]{column},
			color,
			comparisonOperator,
			comparisonValue
		);
	}

	public Sheet addSheetConditionalFormatting(int[] columns, IndexedColors color, byte comparisonOperator, String... comparisonValue) {
		return addSheetConditionalFormatting(
			columns,
			color,
			comparisonOperator,
			1,
			sheet -> sheet.getLastRowNum() + 1,
			comparisonValue
		);
	}

	public Sheet addSheetConditionalFormatting(
		int[] columns,
		IndexedColors color,
		byte comparisonOperator,
		int rowNumToStart,
		Function<Sheet, Integer> rowNumberToEndSupplier,
		String... comparisonValue
	) {
		Sheet currentSheet = getOrCreateSheet(this.currentSheet);
		int rowNumberToStart =
			currentSheet.getPaneInformation() != null && currentSheet.getPaneInformation().isFreezePane() ? rowNumToStart + 1 : rowNumToStart;
		CellRangeAddress[] cellRangeAddress = new CellRangeAddress[columns.length];
		for (int i = 0; i < cellRangeAddress.length; i++) {
			String columnLetter = CellReference.convertNumToColString(columns[i]);
			 cellRangeAddress[i] =
				CellRangeAddress.valueOf(columnLetter + rowNumberToStart + ":" + columnLetter + rowNumberToEndSupplier.apply(currentSheet));
		}

		SheetConditionalFormatting sheetConditionalFormatting = currentSheet.getSheetConditionalFormatting();
        ConditionalFormattingRule conditionalFormattingRule = comparisonValue.length > 1 ?
    		sheetConditionalFormatting.createConditionalFormattingRule(
				comparisonOperator,
				comparisonValue[0], comparisonValue[1]
			) :
			sheetConditionalFormatting.createConditionalFormattingRule(
				comparisonOperator,
				comparisonValue[0]
			);
        FontFormatting fontFormatting =
        		conditionalFormattingRule.createFontFormatting();
        //fontFormatting.setFontColorIndex(IndexedColors.RED.getIndex());
        fontFormatting.setFontStyle(false, true);

        PatternFormatting patternFormatting =
        		conditionalFormattingRule.createPatternFormatting();
        patternFormatting.setFillBackgroundColor(color.index);

        sheetConditionalFormatting.addConditionalFormatting(
    		cellRangeAddress,
    		conditionalFormattingRule
		);
        return currentSheet;
	}

	@Override
	public void close() throws IOException {
		if (workbook != null) {
			workbook.close();
		}
	}

}