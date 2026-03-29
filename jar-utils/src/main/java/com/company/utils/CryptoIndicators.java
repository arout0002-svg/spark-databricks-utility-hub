package com.company.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Reusable technical indicators for crypto market analysis and generic time series.
 */
public final class CryptoIndicators {

    private CryptoIndicators() {
    }

    public static List<Double> movingAverage(List<Double> values, int period) {
        validate(values, period);
        List<Double> result = new ArrayList<>(Collections.nCopies(values.size(), null));
        double rollingSum = 0.0;

        for (int i = 0; i < values.size(); i++) {
            rollingSum += values.get(i);
            if (i >= period) {
                rollingSum -= values.get(i - period);
            }
            if (i >= period - 1) {
                result.set(i, rollingSum / period);
            }
        }
        return result;
    }

    public static List<Double> rsi(List<Double> closes, int period) {
        validate(closes, period);
        List<Double> result = new ArrayList<>(Collections.nCopies(closes.size(), null));

        for (int i = period; i < closes.size(); i++) {
            double gains = 0.0;
            double losses = 0.0;
            for (int j = i - period + 1; j <= i; j++) {
                double delta = closes.get(j) - closes.get(j - 1);
                if (delta >= 0) {
                    gains += delta;
                } else {
                    losses += -delta;
                }
            }
            double avgGain = gains / period;
            double avgLoss = losses / period;
            double rsi = avgLoss == 0.0 ? 100.0 : 100.0 - (100.0 / (1.0 + (avgGain / avgLoss)));
            result.set(i, rsi);
        }
        return result;
    }

    public static List<Double> macd(List<Double> closes, int shortPeriod, int longPeriod) {
        validate(closes, shortPeriod);
        validate(closes, longPeriod);
        if (shortPeriod >= longPeriod) {
            throw new IllegalArgumentException("shortPeriod must be less than longPeriod");
        }

        List<Double> shortMa = movingAverage(closes, shortPeriod);
        List<Double> longMa = movingAverage(closes, longPeriod);
        List<Double> macd = new ArrayList<>(Collections.nCopies(closes.size(), null));
        for (int i = 0; i < closes.size(); i++) {
            Double s = shortMa.get(i);
            Double l = longMa.get(i);
            if (s != null && l != null) {
                macd.set(i, s - l);
            }
        }
        return macd;
    }

    public static double[] bollingerBands(List<Double> closes, int period, double stdDevMultiplier) {
        validate(closes, period);
        List<Double> window = closes.subList(closes.size() - period, closes.size());
        double mean = window.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
        double variance = window.stream()
                .mapToDouble(v -> (v - mean) * (v - mean))
                .average()
                .orElse(0.0);
        double stdDev = Math.sqrt(variance);
        double upper = mean + stdDevMultiplier * stdDev;
        double lower = mean - stdDevMultiplier * stdDev;
        return new double[]{lower, mean, upper};
    }

    private static void validate(List<Double> values, int period) {
        if (values == null || values.isEmpty()) {
            throw new IllegalArgumentException("Input values must not be null or empty");
        }
        if (period <= 0) {
            throw new IllegalArgumentException("Period must be greater than zero");
        }
        if (values.size() < period) {
            throw new IllegalArgumentException("Input size must be >= period");
        }
    }
}
