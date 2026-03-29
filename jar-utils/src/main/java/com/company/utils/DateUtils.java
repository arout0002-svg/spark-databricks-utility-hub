package com.company.utils;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * Date and timestamp utility methods for shared data engineering logic.
 */
public final class DateUtils {
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;

    private DateUtils() {
    }

    public static long toEpochMillis(String isoTimestamp) {
        try {
            return Instant.parse(isoTimestamp).toEpochMilli();
        } catch (DateTimeParseException ex) {
            throw new IllegalArgumentException("Invalid ISO timestamp: " + isoTimestamp, ex);
        }
    }

    public static String normalizeDate(String dateValue) {
        try {
            LocalDate date = LocalDate.parse(dateValue, DATE_FORMATTER);
            return date.format(DATE_FORMATTER);
        } catch (DateTimeParseException ex) {
            throw new IllegalArgumentException("Invalid date format, expected yyyy-MM-dd: " + dateValue, ex);
        }
    }

    public static String epochMillisToIso(long epochMillis) {
        return Instant.ofEpochMilli(epochMillis).atOffset(ZoneOffset.UTC).toString();
    }
}
