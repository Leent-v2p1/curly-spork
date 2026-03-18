package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.history.evaluator;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.history.HistoricalData;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.history.JoinStrategy;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.history.UpdateExpression;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.history.History;

import java.util.Collections;
import java.util.Set;
import java.util.function.Supplier;

import static org.apache.spark.sql.functions.lit;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.SparkSQLUtil.unionAll;

/**
 * Реализация полного обновления витрины
 * История строится на основе полного сравнения изменений
 * Данные делятся на
 * - новые (currentNew)
 * - обновления (currentActual)
 * - прошлые, совпадающие по хэш с обновлением  (prevSame)
 * - прошлые, ставшие не актуальными  (prevNotActual)
 * - прошлые, удаленные (prevDeleted)
 * в actual часть попадают записи: currentNew, currentActual, prevSame
 * в not actual часть попадают записи: prevNotActual, prevDeleted
 */
public class FullMergeEvaluator extends HistoryEvaluator {
    private Dataset<Row> prevSame;
    private Dataset<Row> currentActual;
    private Dataset<Row> currentNew;
    private Dataset<Row> prevNotActual;
    private Dataset<Row> prevDeleted;

    public FullMergeEvaluator(History history, Dataset<Row> curr, Dataset<Row> prev, JoinStrategy joinStrategy) {
        super(history, curr, prev, joinStrategy);
    }

    @Override
    final HistoricalData createPartialHistoricalData() {
        final Dataset<Row> newPartitions = unionAll(currentActual, currentNew);
        final Dataset<Row> newValuesDates = getPartitionDates(newPartitions);
        final Dataset<Row> staleDates = getPartitionDates(prevNotActual);
        final Dataset<Row> deletedDates = getPartitionDates(prevDeleted);
        final Dataset<Row> allDates = unionAll(newValuesDates, staleDates, deletedDates).distinct();

        Supplier<Dataset<Row>> actual = unionActualPartitions(prevSame, newPartitions, allDates);
        Supplier<Dataset<Row>> notActual = () -> unionAll(prevNotActual, prevDeleted).drop(history.getColumnsToDelete());
        Supplier<Set<String>> dates = datesAsSet(allDates);

        return new HistoricalData(actual, notActual, dates);
    }

    @Override
    final HistoricalData createHistoricalData() {
        Supplier<Dataset<Row>> actual = () -> unionAll(prevSame, currentActual, currentNew).drop(history.getColumnsToDelete());
        Supplier<Dataset<Row>> notActual = () -> unionAll(prevNotActual, prevDeleted).drop(history.getColumnsToDelete());
        Supplier<Set<String>> dates = Collections::emptySet;

        return new HistoricalData(actual, notActual, dates);
    }

    @Override
    final void initialization() {
        final Column[] currIds = history.getIdCols(curr);
        final Column[] prevIds = history.getIdCols(prev);

        Column joinCondition = joinStrategy.condition(currIds, prevIds);

        Dataset<Row> currWithAdditional = history.joinAdditionalTables(curr);
        final Column isRowUpdated = UpdateExpression.rowUpdatedViaSha2().expr(prev, curr);
        Dataset<Row> join = currWithAdditional.join(prev, joinCondition, "full")
                .withColumn(IS_UPDATED_COLUMN, isRowUpdated);

        if (!joinStrategy.storageLevel().equals(StorageLevel.NONE())) {
            join = join.persist(joinStrategy.storageLevel());
        }

        Dataset<Row> newRows = join.where(joinStrategy.isNull(prevIds));
        Dataset<Row> oldRows = join.where(joinStrategy.isNull(currIds));
        Dataset<Row> updatedRows = join.where(joinStrategy.isNotNull(prevIds)
                .and(joinStrategy.isNotNull(currIds))
                .and(join.col(IS_UPDATED_COLUMN).equalTo(lit(true))));
        Dataset<Row> notUpdatedRows = join.where(join.col(IS_UPDATED_COLUMN).equalTo(lit(false)));

        prevSame = notUpdatedRows.select(prev.col("*"));
        currentActual = updatedRows.select(history.actualValues(prev, curr));
        currentNew = newRows.select(history.initValues(curr));
        prevNotActual = updatedRows.select(history.staleValues(prev, curr));
        prevDeleted = joinAdditionalTablesToPrev(oldRows).select(history.deletedValues(prev, curr));
    }
}
