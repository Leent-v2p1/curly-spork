package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.history;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.DatamartNaming;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.save.TempSaver;

import java.util.Arrays;
import java.util.Iterator;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.hive.HiveDatamartContext.TEMP_TABLE_POSTFIX;

/**
 * Класс содержит методы для проверки ключей витрины
 */
public class KeyChecker {

    private final Logger log = LoggerFactory.getLogger(KeyChecker.class);
    private final TempSaver tempSaver;
    private final DatamartNaming naming;

    public KeyChecker(TempSaver tempSaver, DatamartNaming naming) {
        this.tempSaver = tempSaver;
        this.naming = naming;
    }

    /**
     * Метод проверяет, что витрина имеет уникальный ключ (обычный или составной),
     * если ключ не уникальный класс бросает ошибку с информацией о не уникальных ключах
     * и сохраняет эти ключи во временную таблицу
     */
    public void checkKeysAreUnique(Dataset<Row> datamart, Column[] idColumns) {
        final Dataset<Row> notUniqueKeys = datamart
                .groupBy(idColumns)
                .agg(
                        count("*").as("rows_with_key")
                )
                .where(col("rows_with_key").gt(1));
        final long numberOfNotUniqueKeys = notUniqueKeys.count();
        final String fullTableName = naming.fullTableName();
        if (numberOfNotUniqueKeys == 0) {
            log.info("All keys in datamart {} are unique", fullTableName);
        } else {
            final String notUniqueKeysTableName = "keys_" + naming.resultTable();
            final String resultDatamart = naming.resultTable() + "_" + TEMP_TABLE_POSTFIX;
            tempSaver.saveTemp(notUniqueKeys, notUniqueKeysTableName);
            String errorMessage = "Datamart " + fullTableName + " has " + numberOfNotUniqueKeys + " not unique keys. " +
                    "Check " + notUniqueKeysTableName + " to get only not unique keys. " +
                    "Check " + resultDatamart + " to get result datamart.";
            throw new IllegalStateException(errorMessage);
        }
    }

    /**
     * Метод проверяет, что витрина не имеет null ключей,
     * иначе бросает ошибку с информацией и сохраняет эти ключи во временную таблицу
     */
    public void checkKeysNotNull(Dataset<Row> datamart, Column[] idColumns) {
        final String fullTableName = naming.fullTableName();
        if (idColumns.length == 0) {
            throw new IllegalStateException("Datamart " + fullTableName + " has empty keys.");
        }

        Iterator<Column> columnIterator = Arrays.asList(idColumns).iterator();
        Column filterExpression = columnIterator.next().isNull();
        while (columnIterator.hasNext()) {
            filterExpression = filterExpression
                    .or(columnIterator.next().isNull());
        }
        final Dataset<Row> nullKeys = datamart
                .where(filterExpression);

        final long numberOfNullKeys = nullKeys.count();
        if (numberOfNullKeys == 0) {
            log.info("All keys in datamart {} are not null", fullTableName);
        } else {
            final String nullKeysTableName = "nullkeys_" + naming.resultTable();
            final String resultDatamart = naming.resultTable() + "_" + TEMP_TABLE_POSTFIX;
            tempSaver.saveTemp(nullKeys, nullKeysTableName);
            String errorMessage = "Datamart " + fullTableName + " has " + numberOfNullKeys + " null keys. " +
                    "Check " + nullKeysTableName + " to get only null keys. " +
                    "Check " + resultDatamart + " to get result datamart.";
            throw new IllegalStateException(errorMessage);
        }
    }
}
