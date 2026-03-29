package com.company.utils;

import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

/**
 * Helper methods that produce reusable Delta Lake SQL patterns.
 */
public final class DeltaHelpers {

    private DeltaHelpers() {
    }

    public static String mergeSql(
            String targetTable,
            String sourceView,
            String joinCondition,
            String updateSetClause,
            String insertClause
    ) {
        return "MERGE INTO " + targetTable + " AS target\n"
                + "USING " + sourceView + " AS source\n"
                + "ON " + joinCondition + "\n"
                + "WHEN MATCHED THEN UPDATE SET " + updateSetClause + "\n"
                + "WHEN NOT MATCHED THEN INSERT " + insertClause;
    }

    public static String deduplicateSql(String sourceTable, List<String> keyColumns, String orderColumn) {
        if (keyColumns == null || keyColumns.isEmpty()) {
            throw new IllegalArgumentException("keyColumns must not be empty");
        }
        StringJoiner partitionKeys = new StringJoiner(", ");
        keyColumns.forEach(partitionKeys::add);

        return "SELECT * FROM (\n"
                + "  SELECT *, ROW_NUMBER() OVER (PARTITION BY "
                + partitionKeys
                + " ORDER BY "
                + orderColumn
                + " DESC) AS _rn\n"
                + "  FROM "
                + sourceTable
                + "\n"
                + ") t WHERE _rn = 1";
    }

    public static String upsertAssignments(Map<String, String> targetToSourceColumns) {
        if (targetToSourceColumns == null || targetToSourceColumns.isEmpty()) {
            throw new IllegalArgumentException("targetToSourceColumns must not be empty");
        }

        StringJoiner setJoiner = new StringJoiner(", ");
        for (Map.Entry<String, String> e : targetToSourceColumns.entrySet()) {
            setJoiner.add("target." + e.getKey() + " = source." + e.getValue());
        }
        return setJoiner.toString();
    }
}
