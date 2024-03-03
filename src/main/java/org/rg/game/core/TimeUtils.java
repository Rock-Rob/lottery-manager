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

	public static final ZoneId DEFAULT_TIME_ZONE ;
	private static ThreadLocal<SimpleDateFormat> defaultDateFmtForFilePrefix;
	public static DateTimeFormatter dateTimeFormatForBackup;
	private static ThreadLocal<SimpleDateFormat> defaultDateFormat;
	private static ThreadLocal<SimpleDateFormat> alternativeDateFormat;
	public static DateTimeFormatter defaultLocalDateFormat;
	public static DateTimeFormatter defaultLocalDateWithDayNameFormat;
	public static Comparator<Date> reversedDateComparator;
	public static Comparator<LocalDate> reversedLocalDateComparator;

	static {
		DEFAULT_TIME_ZONE = ZoneId.of(System.getenv().getOrDefault("TZ", "Europe/Rome"));
		new Thread(() -> {
			boolean logged = false;
			while (!logged) {
				try {
					LogUtils.INSTANCE.info("Set default time zone to " + DEFAULT_TIME_ZONE);
					logged = true;
				} catch (Throwable exc) {

				}
			}
		}).start();
		defaultDateFmtForFilePrefix = ThreadLocal.withInitial(() -> new SimpleDateFormat("[yyyy][MM][dd]"));
		dateTimeFormatForBackup = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");
		defaultDateFormat = ThreadLocal.withInitial(() -> new SimpleDateFormat("dd/MM/yyyy"));
		alternativeDateFormat = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd"));
		defaultLocalDateFormat = DateTimeFormatter.ofPattern("dd/MM/yyyy");
		defaultLocalDateWithDayNameFormat = DateTimeFormatter.ofPattern("EEEE " + getDefaultDateFormat().toPattern());
		reversedDateComparator = Collections.reverseOrder((dateOne, dateTwo) -> dateOne.compareTo(dateTwo));
		reversedLocalDateComparator = Collections.reverseOrder((dateOne, dateTwo) -> dateOne.compareTo(dateTwo));
	}

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
