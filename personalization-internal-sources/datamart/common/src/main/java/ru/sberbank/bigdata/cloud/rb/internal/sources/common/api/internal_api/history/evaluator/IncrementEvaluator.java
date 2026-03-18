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
 * Реализация инкременетального обновления витрины
 * История строится на основе приходящей дельты изменений
 * Данные делятся на
 * - новые (currentNew)
 * - обновления (currentActual)
 * - прошлые, еще актуальные (prevActual)
 * - прошлые, совпадающие по хэш с обновлением  (prevSame)
 * - прошлые, ставшие не актуальными  (prevNotActual)
 * - удаленные (prevDeleted)
 * - обновленные только доп. полем (additionalActual)
 * в actual часть попадают записи: prevSame, currentActual, currentNew, prevActual, additionalActual
 * в not actual часть попадают записи: prevNotActual, prevDeleted
 */
public class IncrementEvaluator extends HistoryEvaluator {
    private static final String IS_ADDITIONAL_UPDATED = "IS_ADDITIONAL";
    private static final String IS_DELETED = "IS_DELETED";
    private Dataset<Row> prevSame;
    private Dataset<Row> prevActual;
    private Dataset<Row> currentNew;
    private Dataset<Row> prevNotActual;
    private Dataset<Row> currentActual;
    private Dataset<Row> additionalActual;
    private Dataset<Row> prevDeleted;

    public IncrementEvaluator(History history, Dataset<Row> curr, Dataset<Row> prev, JoinStrategy joinStrategy) {
        super(history, curr, prev, joinStrategy);
    }

    @Override
    final HistoricalData createPartialHistoricalData() {
        final Dataset<Row> newPartitions = unionAll(currentNew, currentActual, prevActual, additionalActual);
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
        Supplier<Dataset<Row>> actual = () -> unionAll(prevSame, currentActual, currentNew, prevActual, additionalActual).drop(history.getColumnsToDelete());
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

        final Column isAdditonalUpdated;
        if (history.getCustomUpdateExpr() != null) {
            isAdditonalUpdated = history.getCustomUpdateExpr().expr(prev, currWithAdditional);
        } else {
            isAdditonalUpdated = lit(false);
        }

        final Column isDeleted;
        if (history.getDeleteExpr() != null) {
            isDeleted = history.getDeleteExpr().apply(currWithAdditional);
        } else {
            isDeleted = lit(false);
        }

        Dataset<Row> join = currWithAdditional.join(prev, joinCondition, "full")
                .withColumn(IS_UPDATED_COLUMN, isRowUpdated)
                .withColumn(IS_ADDITIONAL_UPDATED, isAdditonalUpdated)
                .withColumn(IS_DELETED, isDeleted);

        if (!joinStrategy.storageLevel().equals(StorageLevel.NONE())) {
            join = join.persist(joinStrategy.storageLevel());
        }

        Dataset<Row> newRows = join.where(joinStrategy.isNull(prevIds)
                .and(join.col(IS_DELETED).equalTo(lit(false))));
        Dataset<Row> updatedRows = join.where(joinStrategy.isNotNull(prevIds)
                .and(joinStrategy.isNotNull(currIds))
                .and(join.col(IS_UPDATED_COLUMN).equalTo(lit(true)))
                .and(join.col(IS_DELETED).equalTo(lit(false))));
        Dataset<Row> notUpdatedRows = join.where(join.col(IS_UPDATED_COLUMN).equalTo(lit(false))
                .and(join.col(IS_ADDITIONAL_UPDATED).equalTo(lit(false))));
        Dataset<Row> additionalUpdatedRows = join.where((joinStrategy.isNull(currIds)
                .or(join.col(IS_UPDATED_COLUMN).equalTo(lit(false))))
                .and(join.col(IS_ADDITIONAL_UPDATED).equalTo(lit(true))));
        Dataset<Row> oldRows = join.where(joinStrategy.isNull(currIds)
                .and(join.col(IS_ADDITIONAL_UPDATED).equalTo(lit(false))));
        Dataset<Row> updatedDeleted = join.where(joinStrategy.isNotNull(prevIds)
                .and(joinStrategy.isNotNull(currIds))
                .and(join.col(IS_UPDATED_COLUMN).equalTo(lit(true)))
                .and(join.col(IS_DELETED).equalTo(lit(true))));

        currentNew = newRows.select(history.initValues(curr));
        prevNotActual = updatedRows.select(history.staleValues(prev, curr));
        currentActual = updatedRows.select(history.actualValues(prev, curr));
        prevSame = notUpdatedRows.select(prev.col("*"));
        prevActual = oldRows.select(prev.col("*"));
        additionalActual = additionalUpdatedRows.select(history.actualFromPrev(prev, curr));
        prevDeleted = updatedDeleted.select(history.deletedValues(prev, curr));
    }
}
