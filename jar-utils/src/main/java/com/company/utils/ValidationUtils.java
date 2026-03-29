package com.company.utils;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Validation helpers for schema checks and generic data quality checks.
 */
public final class ValidationUtils {

    private ValidationUtils() {
    }

    public static boolean hasRequiredColumns(List<String> columns, List<String> requiredColumns) {
        if (columns == null || requiredColumns == null) {
            return false;
        }
        Set<String> columnSet = new HashSet<>(columns);
        return requiredColumns.stream().allMatch(columnSet::contains);
    }

    public static void validateSchema(Map<String, String> actualSchema, Map<String, String> expectedSchema) {
        if (actualSchema == null || expectedSchema == null) {
            throw new IllegalArgumentException("Schemas must not be null");
        }

        for (Map.Entry<String, String> expectedField : expectedSchema.entrySet()) {
            String actualType = actualSchema.get(expectedField.getKey());
            if (actualType == null) {
                throw new IllegalArgumentException("Missing expected column: " + expectedField.getKey());
            }
            if (!actualType.equalsIgnoreCase(expectedField.getValue())) {
                throw new IllegalArgumentException(
                        "Type mismatch for " + expectedField.getKey()
                                + ", expected=" + expectedField.getValue()
                                + ", actual=" + actualType
                );
            }
        }
    }

    public static void validateNonNull(double[] values, String fieldName) {
        if (values == null) {
            throw new IllegalArgumentException(fieldName + " must not be null");
        }
        for (double value : values) {
            if (Double.isNaN(value)) {
                throw new IllegalArgumentException(fieldName + " contains NaN");
            }
        }
    }
}
