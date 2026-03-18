package ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql;

import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.SparkSQLUtil.isRowValueNull;

public class DatasetFormatter {

    public static String formatDatasetMsg(List<String> cols, List<Row> rows) {
        return formatDatasetMsg(cols, rows, new ArrayList<>());
    }

    public static String formatDatasetMsg(List<String> cols, List<Row> rows, List<Boolean> diff) {
        final StringBuilder sb = new StringBuilder();
        final int numCols = cols.size();

        //move mismatched columns to head of list
        if (!diff.isEmpty()) {
            cols = moveCols(cols, diff);
            diff = moveDiff(diff);
        }

        final int[] colWidths = getColumnWidths(cols, rows, numCols);
        final StringBuilder separatorString = getSeparatorString(colWidths, "+");

        //column names string
        formatHeader(cols, sb, colWidths, separatorString);

        //row strings
        boolean firstStringDiff = true;
        for (Row row : rows) {
            formatRows(cols, sb, colWidths, row);
            firstStringDiff = markIfDiff(diff, sb, colWidths, firstStringDiff);
        }

        //footer
        if (diff.isEmpty()) {
            sb.append(separatorString);
        }

        return sb.toString();
    }

    private static List<String> moveCols(List<String> cols, List<Boolean> diff) {
        List<Pair> pairs = new ArrayList<>();
        for (int i = 1; i < cols.size(); i++) {
            pairs.add(new Pair(cols.get(i), diff.get(i)));
        }
        List<String> tmpCols = pairs.stream()
                .sorted(Comparator.comparing(Pair::getDiff).reversed())
                .map(e -> e.col)
                .collect(toList());
        tmpCols.add(0, cols.get(0));
        cols = tmpCols;
        return cols;
    }

    private static List<Boolean> moveDiff(List<Boolean> diff) {
        Boolean tmp = diff.get(0);
        List<Boolean> tmpDiff = diff.stream()
                .skip(1)
                .sorted(Comparator.reverseOrder())
                .collect(toList());
        tmpDiff.add(0, tmp);
        return tmpDiff;
    }

    private static StringBuilder getSeparatorString(int[] colWidths, String symbol) {
        StringBuilder separatorString = new StringBuilder(symbol);
        for (int width : colWidths) {
            separatorString.append(new String(new char[width]).replace("\0", "-")).append(symbol);
        }
        separatorString.append("\n");
        return separatorString;
    }

    private static void formatHeader(List<String> cols, StringBuilder sb, int[] colWidths, StringBuilder separatorString) {
        sb.append(separatorString);
        for (int i = 0; i < cols.size(); i++) {
            sb.append("|");
            sb.append(cols.get(i)).append(new String(new char[Math.max(colWidths[i] - cols.get(i).length(), 0)]).replace("\0", " "));
        }
        sb.append("|\n");
        sb.append(separatorString);
    }

    private static boolean markIfDiff(List<Boolean> diff, StringBuilder sb, int[] colWidths, boolean firstStringDiff) {
        boolean result = firstStringDiff;
        if (!diff.isEmpty()) {
            if (firstStringDiff) {
                result = false;
            } else {
                sb.append(" ");
                for (int i = 0; i < diff.size(); i++) {
                    sb.append(new String(new char[colWidths[i]]).replace("\0", diff.get(i) ? "^" : " ")).append(" ");
                }
                sb.append("\n\n");
                result = true;
            }
        }
        return result;
    }

    private static void formatRows(List<String> cols, StringBuilder sb, int[] colWidths, Row row) {
        for (int i = 0; i < cols.size(); i++) {
            sb.append("|");
            final String value = getRowValue(cols, row, i);
            sb.append(value).append(new String(new char[colWidths[i] - value.length()]).replace("\0", " "));
        }
        sb.append("|\n");
    }

    private static int[] getColumnWidths(List<String> cols, List<Row> rows, int numCols) {
        final int minColWidth = 3;
        final int[] colWidths = new int[numCols];
        Arrays.fill(colWidths, minColWidth);

        for (int i = 0; i < cols.size(); i++) {
            colWidths[i] = Math.max(colWidths[i], cols.get(i).length());
        }
        for (Row row : rows) {
            for (int i = 0; i < cols.size(); i++) {
                String value = getRowValue(cols, row, i);
                colWidths[i] = Math.max(colWidths[i], value.length());
            }
        }
        return colWidths;
    }

    private static String getRowValue(List<String> cols, Row row, int i) {
        String value;
        if (row.schema() == null) {
            value = row.get(i) == null ? "null" : row.getAs(i).toString();
        } else {
            value = isRowValueNull(row, cols.get(i)) ? "null" : row.getAs(cols.get(i)).toString();
        }
        return value;
    }

    private static class Pair {
        final String col;
        final Boolean diff;

        public Pair(String col, Boolean diff) {
            this.col = col;
            this.diff = diff;
        }

        public Boolean getDiff() {
            return diff;
        }
    }
}