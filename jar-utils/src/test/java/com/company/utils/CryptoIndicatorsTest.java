package com.company.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

class CryptoIndicatorsTest {

    @Test
    void shouldComputeMovingAverage() {
        List<Double> closes = Arrays.asList(10.0, 20.0, 30.0, 40.0, 50.0);
        List<Double> ma = CryptoIndicators.movingAverage(closes, 3);
        Assertions.assertNull(ma.get(1));
        Assertions.assertEquals(20.0, ma.get(2));
        Assertions.assertEquals(40.0, ma.get(4));
    }

    @Test
    void shouldComputeBollingerBands() {
        List<Double> closes = Arrays.asList(10.0, 12.0, 13.0, 15.0, 18.0, 20.0);
        double[] bands = CryptoIndicators.bollingerBands(closes, 5, 2.0);
        Assertions.assertEquals(3, bands.length);
        Assertions.assertTrue(bands[0] < bands[1]);
        Assertions.assertTrue(bands[1] < bands[2]);
    }
}
