package com.company.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class ValidationUtilsTest {

    @Test
    void shouldValidateRequiredColumns() {
        List<String> cols = Arrays.asList("id", "symbol", "close");
        List<String> required = Arrays.asList("id", "close");
        Assertions.assertTrue(ValidationUtils.hasRequiredColumns(cols, required));
    }

    @Test
    void shouldFailOnSchemaMismatch() {
        Map<String, String> actual = new HashMap<>();
        actual.put("id", "string");
        actual.put("price", "double");

        Map<String, String> expected = new HashMap<>();
        expected.put("id", "string");
        expected.put("price", "decimal");

        Assertions.assertThrows(IllegalArgumentException.class, () -> ValidationUtils.validateSchema(actual, expected));
    }
}
