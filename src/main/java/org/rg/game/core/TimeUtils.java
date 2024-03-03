package org.rg.game.core;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;

public class TimeUtils {

	public static final ZoneId DEFAULT_TIME_ZONE = ZoneId.of(System.getenv().getOrDefault("TZ", "Europe/Rome"));
	private static ThreadLocal<SimpleDateFormat> defaultDateFmtForFilePrefix = ThreadLocal.withInitial(() -> new SimpleDateFormat("[yyyy][MM][dd]"));
	public static DateTimeFormatter dateTimeFormatForBackup = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");
	private static ThreadLocal<SimpleDateFormat> defaultDateFormat = ThreadLocal.withInitial(() -> new SimpleDateFormat("dd/MM/yyyy"));
	private static ThreadLocal<SimpleDateFormat> alternativeDateFormat = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd"));
	public static DateTimeFormatter defaultLocalDateFormat = DateTimeFormatter.ofPattern("dd/MM/yyyy");
	public static DateTimeFormatter defaultLocalDateWithDayNameFormat = DateTimeFormatter.ofPattern("EEEE " + getDefaultDateFormat().toPattern());
	public static Comparator<Date> reversedDateComparator = Collections.reverseOrder((dateOne, dateTwo) -> dateOne.compareTo(dateTwo));
	public static Comparator<LocalDate> reversedLocalDateComparator = Collections.reverseOrder((dateOne, dateTwo) -> dateOne.compareTo(dateTwo));

	public static Date toDate(LocalDate date) {
		return Date.from(date.atStartOfDay(DEFAULT_TIME_ZONE).toInstant());
	}

	public static LocalDate toLocalDate(Date date) {
		return date.toInstant().atZone(DEFAULT_TIME_ZONE).toLocalDate();
	}

	public static long differenceInDays(Date startDate, Date endDate) {
		return ChronoUnit.DAYS.between(toLocalDate(startDate), toLocalDate(endDate));
	}

	public static long differenceInDays(LocalDate startDate, LocalDate endDate) {
		return ChronoUnit.DAYS.between(startDate, endDate);
	}

	public static boolean isBetween(Date source, Date startDate, Date endDate) {
		return startDate.compareTo(source) <= 0 && endDate.compareTo(source) >= 0;
	}

	public static SimpleDateFormat getDefaultDateFormat() {
		return defaultDateFormat.get();
	}

	public static SimpleDateFormat getAlternativeDateFormat() {
		return alternativeDateFormat.get();
	}

	public static SimpleDateFormat getDefaultDateFmtForFilePrefix() {
		return defaultDateFmtForFilePrefix.get();
	}

	public static Date increment(Date dateCellValue, Integer daysOffset, ChronoUnit chronoUnit) {
		return toDate(toLocalDate(dateCellValue).plus(daysOffset, chronoUnit));
	}

	public static LocalDate today() {
		return LocalDate.now(DEFAULT_TIME_ZONE);
	}

	public static LocalDateTime now() {
		return LocalDateTime.now(DEFAULT_TIME_ZONE);
	}

}
