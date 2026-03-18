package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.history.evaluator;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.history.HistoricalData;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.history.JoinStrategy;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.history.History;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_strategy.SeparateHistoryTableSavingStrategy;

import java.util.Arrays;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.FieldConstants.MONTH_PART_FORMAT;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.SparkSQLUtil.unionAll;

public abstract class HistoryEvaluator {
    public static final String IS_UPDATED_COLUMN = "IS_UPDATED";
    private final Logger log = LoggerFactory.getLogger(HistoryEvaluator.class);
    final Dataset<Row> curr;
    final Dataset<Row> prev;
    final JoinStrategy joinStrategy;
    History history;

    HistoryEvaluator(History history, Dataset<Row> curr, Dataset<Row> prev, JoinStrategy joinStrategy) {
        this.history = history;
        this.curr = curr;
        this.prev = prev;
        this.joinStrategy = joinStrategy;
    }

    public final HistoricalData evaluate() {
        initialization();
        if (Boolean.parseBoolean(System.getProperty("partiallyHistoryUpdate"))) {
            return createPartialHistoricalData();
        } else {
            return createHistoricalData();
        }
    }

    abstract void initialization();

    abstract HistoricalData createPartialHistoricalData();

    abstract HistoricalData createHistoricalData();

    final Supplier<Dataset<Row>> unionActualPartitions(final Dataset<Row> actualPartitions,
                                                       final Dataset<Row> newPartitions,
                                                       final Dataset<Row> allDates) {
        return () -> {
            log.info("using partiallyHistoryUpdate");
            final Dataset<Row> prevPartitions = actualPartitions
                    .join(allDates, date_format(actualPartitions.col("row_actual_from_dt"), MONTH_PART_FORMAT).equalTo(allDates.col(SeparateHistoryTableSavingStrategy.ACTUAL_PARTITION_COLUMN)), "leftsemi");

            return unionAll(newPartitions, prevPartitions).drop(history.getColumnsToDelete());
        };
    }

    final Supplier<Set<String>> datesAsSet(final Dataset<Row> allDates) {
        return () -> allDates
                .collectAsList()
                .stream()
                .map(row -> SeparateHistoryTableSavingStrategy.ACTUAL_PARTITION_COLUMN + "=" + row.getString(0))
                .collect(toSet());
    }

    final Dataset<Row> getPartitionDates(Dataset<Row> df) {
        return df
                .select(date_format(df.col("row_actual_from_dt"), MONTH_PART_FORMAT).as(SeparateHistoryTableSavingStrategy.ACTUAL_PARTITION_COLUMN))
                .distinct();
    }

    final Dataset<Row> joinAdditionalTablesToPrev(Dataset<Row> joinedDf) {
        final Column[] prevColumns =
                Stream.concat(
                        Arrays.stream(prev.columns()).map(prev::col),
                        Stream.of(col(IS_UPDATED_COLUMN)))
                        .toArray(Column[]::new);
        final Dataset<Row> prevDatamart = joinedDf.select(prevColumns);
        return history.joinAdditionalTables(prevDatamart);
    }
}
