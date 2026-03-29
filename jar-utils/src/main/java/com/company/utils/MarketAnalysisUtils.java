package com.company.utils;

import java.util.Locale;
import java.util.Objects;

/**
 * Generic utility methods useful across market and non-market ETL workloads.
 */
public final class MarketAnalysisUtils {

    private MarketAnalysisUtils() {
    }

    public static String normalizeSymbol(String symbol) {
        if (symbol == null || symbol.isBlank()) {
            return "UNKNOWN";
        }
        return symbol.trim().toUpperCase(Locale.ROOT);
    }

    public static String nullToDefault(String value, String defaultValue) {
        return value == null || value.isBlank() ? defaultValue : value;
    }

    public static double safeDivide(double numerator, double denominator, double defaultValue) {
        if (denominator == 0.0) {
            return defaultValue;
        }
        return numerator / denominator;
    }

    public static boolean isValidPrice(Double price) {
        return price != null && !price.isNaN() && price > 0;
    }

    public static boolean allNonNull(Object... values) {
        for (Object value : values) {
            if (Objects.isNull(value)) {
                return false;
            }
        }
        return true;
    }
}
