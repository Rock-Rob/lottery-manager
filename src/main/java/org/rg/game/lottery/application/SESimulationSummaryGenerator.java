package org.rg.game.lottery.application;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.poi.common.usermodel.HyperlinkType;
import org.apache.poi.ss.formula.BaseFormulaEvaluator;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.CellValue;
import org.apache.poi.ss.usermodel.ComparisonOperator;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.rg.game.core.FirestoreWrapper;
import org.rg.game.core.LogUtils;
import org.rg.game.core.ResourceUtils;
import org.rg.game.core.TimeUtils;
import org.rg.game.lottery.engine.Premium;
import org.rg.game.lottery.engine.SimpleWorkbookTemplate;

public class SESimulationSummaryGenerator extends Shared {
	static final String EXTRACTION_COUNTER_LABEL = "Conteggio estrazioni";
	static final String SYSTEM_COUNTER_LABEL = "Conteggio sistemi";

	public static void main(String[] args) {
		try {
			String simulationSummaryFolder = Arrays.stream(
				ResourceUtils.INSTANCE.pathsFromSystemEnv(
						"working-path.simulations.folder"
					)
				).findFirst().orElseGet(() -> null);
			LogUtils.INSTANCE.info("\n\n");
			String simulationSummaryFile = simulationSummaryFolder + File.separator + "Summary.xlsx";
			SimpleWorkbookTemplate workBookTemplate = new SimpleWorkbookTemplate(true);
			Sheet summarySheet = workBookTemplate.getOrCreateSheet("Riepilogo", true);
			List<String> headerLabels = new ArrayList<>();
			List<String> headerLabelsTemp = new ArrayList<>(SELotterySimpleSimulator.reportHeaderLabels);
			List<String> headersToBeSkipped = new ArrayList<>(
				Arrays.asList(
					SELotterySimpleSimulator.HISTORICAL_UPDATE_DATE_LABEL,
					SELotterySimpleSimulator.FILE_LABEL
				)
			);
			headerLabelsTemp.removeAll(headersToBeSkipped);
			headerLabels.add(SELotterySimpleSimulator.FILE_LABEL);
			headerLabels.addAll(headerLabelsTemp);
			headerLabels.set(headerLabels.indexOf(SELotterySimpleSimulator.EXTRACTION_DATE_LABEL), SYSTEM_COUNTER_LABEL);
			headerLabels.add(headerLabels.indexOf(SYSTEM_COUNTER_LABEL), EXTRACTION_COUNTER_LABEL);
			headerLabels.add(SELotterySimpleSimulator.HISTORICAL_UPDATE_DATE_LABEL);
			headersToBeSkipped.add(SELotterySimpleSimulator.EXTRACTION_DATE_LABEL);
			workBookTemplate.createHeader(true, headerLabels);
			AtomicInteger reportCounter = new AtomicInteger(0);
			process(
				null,
				simulationSummaryFolder,
				workBookTemplate,
				headersToBeSkipped,
				reportCounter
			);
			summarySheet.setColumnWidth(getCellIndex(summarySheet, SELotterySimpleSimulator.FILE_LABEL), 23000);
			summarySheet.setColumnWidth(getCellIndex(summarySheet, SELotterySimpleSimulator.FOLLOWING_PROGRESSIVE_HISTORICAL_COST_LABEL), 3000);
			summarySheet.setColumnWidth(getCellIndex(summarySheet, SELotterySimpleSimulator.FOLLOWING_PROGRESSIVE_HISTORICAL_RETURN_LABEL), 3000);
			summarySheet.setColumnWidth(getCellIndex(summarySheet, SELotterySimpleSimulator.HISTORICAL_COST_LABEL), 3000);
			summarySheet.setColumnWidth(getCellIndex(summarySheet, SELotterySimpleSimulator.HISTORICAL_RETURN_LABEL), 3300);
			summarySheet.setColumnWidth(getCellIndex(summarySheet, SELotterySimpleSimulator.HISTORICAL_UPDATE_DATE_LABEL), 18000);
			try (OutputStream destFileOutputStream = new FileOutputStream(simulationSummaryFile)){
				workBookTemplate.addSheetConditionalFormatting(
					new int[] {
						getCellIndex(summarySheet, Premium.LABEL_FIVE),
						getCellIndex(summarySheet, SELotterySimpleSimulator.getFollowingProgressiveHistoricalPremiumLabel(Premium.LABEL_FIVE))
					},
					IndexedColors.YELLOW,
					ComparisonOperator.GT,
					"0"
				);
				workBookTemplate.addSheetConditionalFormatting(
					new int[] {
						getCellIndex(summarySheet, Premium.LABEL_FIVE_PLUS),
						getCellIndex(summarySheet, SELotterySimpleSimulator.getFollowingProgressiveHistoricalPremiumLabel(Premium.LABEL_FIVE_PLUS))
					},
					IndexedColors.ORANGE,
					ComparisonOperator.GT,
					"0"
				);
				workBookTemplate.addSheetConditionalFormatting(
					new int[] {
						getCellIndex(summarySheet, Premium.LABEL_SIX),
						getCellIndex(summarySheet, SELotterySimpleSimulator.getFollowingProgressiveHistoricalPremiumLabel(Premium.LABEL_SIX))
					},
					IndexedColors.RED,
					ComparisonOperator.GT,
					"0"
				);
				workBookTemplate.setAutoFilter(0, reportCounter.get(), 0, headerLabels.size() - 1);
				BaseFormulaEvaluator.evaluateAllFormulaCells(workBookTemplate.getWorkbook());
				workBookTemplate.getWorkbook().write(destFileOutputStream);
			}
			LogUtils.INSTANCE.info("\n\nSummary file succesfully generated");
		} catch (Throwable exc) {
			LogUtils.INSTANCE.error("\n\nUnable to generate summary file");
			LogUtils.INSTANCE.error(exc);
		}
		FirestoreWrapper.shutdownDefaultInstance();
	}

	protected static void process(
		String currentRelativePath,
		String folderAbsolutePath,
		SimpleWorkbookTemplate workBookTemplate,
		List<String> headersToBeSkipped,
		AtomicInteger reportCounter
	) {
		CellStyle centerAligned = workBookTemplate.getWorkbook().createCellStyle();
		centerAligned.setAlignment(HorizontalAlignment.CENTER);
		for (File singleSimFolder : new File(folderAbsolutePath).listFiles(
			(file, name) -> {
				File currentIteratedFile = new File(file, name);
				return currentIteratedFile.isDirectory() &&
					!currentIteratedFile.getName().equals(SELotterySimpleSimulator.DATA_FOLDER_NAME) &&
					!currentIteratedFile.getName().equals(SELotteryComplexSimulator.GENERATED_FOLDER_NAME);
			})
		) {
			String singleSimFolderRelPath = Optional.ofNullable(currentRelativePath).map(cRP -> cRP + "/").orElseGet(() -> "") + singleSimFolder.getName();
			LogUtils.INSTANCE.info("Scanning " + singleSimFolder.getAbsolutePath());
			File report = Arrays.stream(singleSimFolder.listFiles((file, name) -> name.endsWith("report.xlsx"))).findFirst().orElseGet(() -> null);
			if (report != null) {
				reportCounter.incrementAndGet();
				try {
					process(
						singleSimFolderRelPath,
						workBookTemplate,
						centerAligned,
						headersToBeSkipped,
						singleSimFolder,
						report
					);
				} catch (Throwable exc) {
					LogUtils.INSTANCE.warn("Unable to process " + report.getAbsolutePath() + ": " + exc.getMessage());
					List<File> backupFiles = ResourceUtils.INSTANCE.findReverseOrdered("report - ", "xlsx", report.getParentFile().getAbsolutePath());
					boolean processed = false;
					if (backupFiles.size() > 0) {
						LogUtils.INSTANCE.info("Trying to process its backups");
						for (File backup : backupFiles) {
							try {
								process(
									singleSimFolderRelPath,
									workBookTemplate,
									centerAligned,
									headersToBeSkipped,
									singleSimFolder,
									backup
								);
								processed = true;
								break;
							} catch (Throwable e) {

							}
						}
					}
					if (!processed) {
						LogUtils.INSTANCE.error("Unable to process backups of " + report.getAbsolutePath());
					}
				}
			} else {
				LogUtils.INSTANCE.warn("No report found in folder " + singleSimFolder.getAbsolutePath());
			}
			process(
				singleSimFolderRelPath,
				singleSimFolder.getAbsolutePath(),
				workBookTemplate,
				headersToBeSkipped,
				reportCounter
			);
		}
	}

	protected static void process(
		String singleSimFolderRelPath,
		SimpleWorkbookTemplate summaryWorkBookTemplate,
		CellStyle centerAligned,
		List<String> headersToBeSkipped,
		File singleSimFolder,
		File report
	) throws IOException {
		try (InputStream inputStream = new FileInputStream(report.getAbsolutePath());Workbook simulationWorkBook = new XSSFWorkbook(inputStream);) {
			FormulaEvaluator evaluator = simulationWorkBook.getCreationHelper().createFormulaEvaluator();
			Sheet resultSheet = simulationWorkBook.getSheet(SELotterySimpleSimulator.RESULTS_LABEL);
			int historicalUpdateDateColumnIndex = getCellIndex(resultSheet, SELotterySimpleSimulator.HISTORICAL_UPDATE_DATE_LABEL);
			summaryWorkBookTemplate.addRow();
			Cell cellForName = summaryWorkBookTemplate.addCell(report.getName()).get(0);
			summaryWorkBookTemplate.setLinkForCell(
				HyperlinkType.FILE,
				cellForName,
				singleSimFolderRelPath + "/" + report.getName()
			);
			Set<Date> extractionDatesHolder = new LinkedHashSet<>();
			int generatedSystemCounter = 0;
			Set<Date> historicalUpdateDates = new TreeSet<>();
			for (int i = SELotterySimpleSimulator.getHeaderSize(); i < resultSheet.getLastRowNum() + 1; i++) {
				generatedSystemCounter++;
				extractionDatesHolder.add(resultSheet.getRow(i).getCell(0).getDateCellValue());
				Date historicalUpdateDate = resultSheet.getRow(i).getCell(historicalUpdateDateColumnIndex).getDateCellValue();
				if (historicalUpdateDate != null) {
					historicalUpdateDates.add(historicalUpdateDate);
				}
			}
			/*if (!historicalUpdateDates.isEmpty()) {
				historicalUpdateDates = new LinkedHashSet<>(Arrays.asList(historicalUpdateDates.iterator().next()));
			}*/
			summaryWorkBookTemplate.addCell(extractionDatesHolder.size(), "#,##0");
			summaryWorkBookTemplate.addCell(generatedSystemCounter, "#,##0");
			for (String cellLabel : SELotterySimpleSimulator.reportHeaderLabels) {
				if (!headersToBeSkipped.contains(cellLabel)) {
					Cell simulationCell = resultSheet.getRow(1).getCell(getCellIndex(resultSheet, cellLabel));
					CellValue simulationCellValue = evaluator.evaluate(simulationCell);
					if (simulationCellValue.getCellType().equals(CellType.NUMERIC)) {
						summaryWorkBookTemplate.addCell(simulationCellValue.getNumberValue(), "#,##0");
					} else if (simulationCellValue.getCellType().equals(CellType.STRING)) {
						summaryWorkBookTemplate.addCell(simulationCellValue.getStringValue()).stream().findFirst().get().getCellStyle().setAlignment(HorizontalAlignment.RIGHT);
					}
				}
			}
			if (!historicalUpdateDates.isEmpty()) {
				summaryWorkBookTemplate.addCell(String.join(", ", historicalUpdateDates.stream().map(TimeUtils.getDefaultDateFormat()::format).collect(Collectors.toList())))
				.stream().findFirst().get().setCellStyle(centerAligned);
			}
		}
	}

}
