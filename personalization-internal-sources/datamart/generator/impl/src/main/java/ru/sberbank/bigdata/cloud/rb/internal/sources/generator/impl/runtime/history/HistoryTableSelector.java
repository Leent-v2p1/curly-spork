package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.runtime.history;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.DatamartContext;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.SysPropertyTool;
import scala.Tuple2;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Comparator.comparing;

/**
 * Класс
 * Created by sbt-buylin-ma on 15.03.2017.
 */
class HistoryTableSelector {
    private static final String HISTORY_SNAPSHOT_DATE_FORMAT = "yyyyMMddHHmmss";
    private final DatamartContext context;
    private final FullTableName historyTableName;
    private List<FullTableName> tables;

    HistoryTableSelector(DatamartContext context, String fullTableName) {
        this.context = context;
        this.historyTableName = FullTableName.of(fullTableName);
    }

    private void findHistoryTables() {
        List<String> tablesInDb = context.schemaTables(historyTableName.dbName());
        tables = tablesInDb
                .stream()
                .filter(tableName -> tableName.startsWith(historyTableName.tableName()))
                .map(tableName -> FullTableName.of(historyTableName.dbName(), tableName))
                .collect(Collectors.toList());
    }

    FullTableName selectHistoryTable(LocalDate day) {
        if (tables == null) {
            findHistoryTables();
        }
        if (tables.isEmpty()) {
            throw new IllegalStateException("No history table for day " + day);
        }

        if (Boolean.valueOf(SysPropertyTool.getSystemProperty("spark.history.stage.debug"))) {
            return tables.stream()
                    .map(tableName -> new Tuple2<>(tableName, extractDateString(tableName)))
                    .filter(tableNameDateStringTuple -> tableNameDateStringTuple._2.length() == HISTORY_SNAPSHOT_DATE_FORMAT.length())
                    .map(tuple -> new TableAndTime(LocalDateTime.parse(tuple._2, DateTimeFormatter.ofPattern(HISTORY_SNAPSHOT_DATE_FORMAT)), tuple._1))
                    .max(comparing(tableAndTime -> tableAndTime.time))
                    .orElseThrow(() -> new IllegalStateException("No history table for day " + day))
                    .tableName;
        }

        LocalDateTime dayMidnight = LocalDateTime.of(day, LocalTime.MIDNIGHT);
        return tables.stream()
                .map(table -> new TableAndTime(getHistoryTableDate(table), table))
                .filter(table -> table.time.isBefore(dayMidnight))
                .max(comparing(tableAndTime -> tableAndTime.time))
                .orElseThrow(() -> new IllegalStateException("No history table for day " + day))
                .tableName;
    }

    private String extractDateString(FullTableName tableName) {
        final String fullTableName = tableName.fullTableName();
        final int underscoreLastIndex = fullTableName.lastIndexOf('_');
        final String dateString;
        if (underscoreLastIndex != -1) {
            dateString = fullTableName.substring(underscoreLastIndex + 1);
        } else {
            throw new IllegalArgumentException("Wrong history source table name format: " + fullTableName
                    + ", format should be 'tableName_yyyyMMddHHmmss'");
        }
        return dateString;
    }

    private LocalDateTime getHistoryTableDate(FullTableName historyTable) {
        String[] tableNameParts = historyTable.fullTableName().split("_"); //tableName format should be 'name_${timestamp_in_seconds}'
        if (tableNameParts.length < 2) {
            throw new IllegalArgumentException("Wrong history source table name format: " + historyTable
                    + ", format should be 'name_${timestamp_in_seconds}'");
        }
        String secondsStr = tableNameParts[tableNameParts.length - 1];
        Long seconds;
        try {
            seconds = Long.valueOf(secondsStr);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Wrong history source table name format: " + historyTable
                    + ", format should be 'name_${timestamp_in_seconds}'");
        }
        return LocalDateTime.ofInstant(Instant.ofEpochSecond(seconds), ZoneId.systemDefault());
    }

    public static class TableAndTime {

        final LocalDateTime time;

        final FullTableName tableName;

        TableAndTime(LocalDateTime time, FullTableName tableName) {
            this.time = time;
            this.tableName = tableName;
        }
    }
}
