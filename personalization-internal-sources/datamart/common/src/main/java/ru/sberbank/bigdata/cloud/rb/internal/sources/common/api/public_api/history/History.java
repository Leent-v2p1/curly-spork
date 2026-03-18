package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.history;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.history.DefaultJoinStrategy;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.history.HistoricalData;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.history.JoinStrategy;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.history.UpdateExpression;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.history.evaluator.FullMergeEvaluator;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.history.evaluator.IncrementEvaluator;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.DateConstants;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.date_format;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_strategy.SeparateHistoryTableSavingStrategy.ACTUAL_PARTITION_COLUMN;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_strategy.SeparateHistoryTableSavingStrategy.HISTORY_PARTITION_COLUMN;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.FieldConstants.MONTH_PART_FORMAT;

public class History {

    private final List<HistColumn> histColumns = new ArrayList<>();
    private final List<HistColumn> snpHistColumns = new ArrayList<>();

    private final List<AdditionalTable> additionalTables = new ArrayList<>();

    private final String[] ids;
    private String[] columnsToDelete;
    private JoinStrategy joinStrategy;
    private UpdateExpression customUpdateExpression;
    private Function<Dataset<Row>, Column> deleteExpression;
    private boolean incremental;

    private History(String[] ids) {
        this.ids = ids;
    }

    public static HistBuilder builder(Integer ctlLoadingId, String... ids) {
        return new HistBuilder(ctlLoadingId, ids);
    }

    public static History defaultHistory(Integer ctlLoadingId, String id, LocalDate replicaActualityDate) {
        return builder(ctlLoadingId, id).create(replicaActualityDate);
    }

    public Dataset<Row> applyDefault(Dataset<Row> nonHistorical) {
        List<Column> cols = new ArrayList<>(Collections.singletonList(nonHistorical.col("*")));
        Dataset<Row> nonHistoricalWithAdditionalTables = joinAdditionalTables(nonHistorical).cache();
        for (HistColumn col : snpHistColumns) {
            cols.add(col.initValue(col.isNeedsAdditionalTables() ? nonHistoricalWithAdditionalTables : nonHistorical).as(col.name()));
        }
        nonHistoricalWithAdditionalTables = nonHistoricalWithAdditionalTables.select(cols.toArray(new Column[]{}));
        if (joinStrategy != null && !joinStrategy.storageLevel().equals(StorageLevel.NONE())) {
            nonHistoricalWithAdditionalTables = nonHistoricalWithAdditionalTables.persist(joinStrategy.storageLevel());
        }
        return nonHistoricalWithAdditionalTables.drop(columnsToDelete);
    }

    public HistoricalData evalHistory(Dataset<Row> newDatamart, Dataset<Row> prevDatamart) {
        if (incremental) {
            return new IncrementEvaluator(
                    this,
                    newDatamart,
                    prevDatamart,
                    joinStrategy != null ? joinStrategy : new DefaultJoinStrategy())
                    .evaluate();
        } else {
            return new FullMergeEvaluator(
                    this,
                    newDatamart,
                    prevDatamart,
                    joinStrategy != null ? joinStrategy : new DefaultJoinStrategy())
                    .evaluate();
        }
    }

    public Dataset<Row> joinAdditionalTables(Dataset<Row> frame) {
        for (AdditionalTable table : additionalTables) {
            final Dataset<Row> df = table.table();
            frame = frame.join(df, table.joinExpr(frame), table.joinType());
        }
        return frame;
    }

    public UpdateExpression getCustomUpdateExpr() {
        return customUpdateExpression;
    }

    public Function<Dataset<Row>, Column> getDeleteExpr() {
        return deleteExpression;
    }

    public String[] getColumnsToDelete() {
        return columnsToDelete;
    }

    public Column[] getIdCols(Dataset<Row> datamart) {
        return Arrays.stream(ids).map(datamart::col).toArray(Column[]::new);
    }

    public Column[] initValues(Dataset<Row> curr) {
        return extractSnp(col -> col.initValue(curr).as(col.name()), curr.col("*"));
    }

    public Column[] actualValues(Dataset<Row> prev, Dataset<Row> curr) {
        return extractSnp(col -> updatedOrSame(prev, curr, col).as(col.name()), curr.col("*"));
    }

    public Column[] staleValues(Dataset<Row> prev, Dataset<Row> curr) {
        return extract(col -> staleOrSame(prev, curr, col).as(col.name()), withoutHistory(prev));
    }

    public Column[] deletedValues(Dataset<Row> prev, Dataset<Row> curr) {
        return extract(col -> col.deletedValue(prev, curr).as(col.name()), withoutHistory(prev));
    }

    public Column[] actualFromPrev(Dataset<Row> prev, Dataset<Row> curr) {
        return extractSnp(col -> updatedOrSame(prev, curr, col).as(col.name()), withoutHistory(prev));
    }

    private Column updatedOrSame(Dataset<Row> prev, Dataset<Row> curr, HistColumn col) {
        return changeIfUpdated(prev, curr, col, col.updatedValue(prev, curr));
    }

    private Column staleOrSame(Dataset<Row> prev, Dataset<Row> curr, HistColumn col) {
        return changeIfUpdated(prev, curr, col, col.staleValue(prev, curr));
    }

    private Column changeIfUpdated(Dataset<Row> prev, Dataset<Row> curr, HistColumn col, Column value) {
        return snpHistColumns.contains(col)
                ? when(col.isUpdated(prev, curr), value).otherwise(prev.col(col.name()))
                : value;
    }

    private Column[] extract(Function<HistColumn, Column> valueExtractor, Column... nonHistoricColumns) {
        Stream<Column> histStream = histColumns.stream()
                .map(valueExtractor);
        return Stream.concat(Stream.of(nonHistoricColumns), histStream)
                .toArray(Column[]::new);
    }

    private Column[] extractSnp(Function<HistColumn, Column> valueExtractor, Column... nonHistoricColumns) {
        Stream<Column> histStream = snpHistColumns.stream()
                .map(valueExtractor);
        return Stream.concat(Stream.of(nonHistoricColumns), histStream)
                .toArray(Column[]::new);
    }

    public Column[] withoutHistory(Dataset<Row> prevDatamart) {
        final List<String> names = nameStream()
                .collect(Collectors.toList());
        return Stream.of(prevDatamart.columns())
                .filter(name -> !names.contains(name))
                .map(prevDatamart::col)
                .toArray(Column[]::new);
    }

    private Stream<String> nameStream() {
        return snpHistColumns.stream().map(HistColumn::name);
    }

    public List<String> getIds() {
        return Arrays.asList(ids);
    }

    public static class HistBuilder {

        private final Integer ctlLoadingId;
        private final History hst;

        private HistBuilder(Integer ctlLoadingId, String[] ids) {
            this.ctlLoadingId = ctlLoadingId;
            hst = new History(ids);
            hst.columnsToDelete = new String[]{};
        }

        public HistBuilder addColumn(HistColumn column) {
            addSnpColumn(column);
            addHstColumn(column);
            return this;
        }

        private void addSnpColumn(HistColumn column) {
            hst.snpHistColumns.add(column);
        }

        private void addHstColumn(HistColumn column) {
            hst.histColumns.add(column);
        }

        public HistBuilder customJoinStrategy(JoinStrategy joinStrategy) {
            hst.joinStrategy = joinStrategy;
            return this;
        }

        public HistBuilder deleteColumnsAfterEvaluation(String... columnNames) {
            hst.columnsToDelete = columnNames;
            return this;
        }

        public HistBuilder additionalTable(Dataset<Row> table, String tableJoinField, String datamartJoinField) {
            return additionalTable(table, tableJoinField, datamartJoinField, "left");
        }

        public HistBuilder additionalTable(Dataset<Row> table, String tableJoinField, String datamartJoinField, String joinType) {
            hst.additionalTables.add(new AdditionalTable(table,
                    datamart -> datamart.col(datamartJoinField).equalTo(table.col(tableJoinField)), joinType));
            return this;
        }

        public HistBuilder additionalTable(Dataset<Row> table, Function<Dataset<Row>, Column> joinExpr) {
            hst.additionalTables.add(new AdditionalTable(table, joinExpr, "left"));
            return this;
        }

        public HistBuilder additionalUpdateExpr(UpdateExpression customUpdateExpression) {
            hst.customUpdateExpression = customUpdateExpression;
            return this;
        }

        public History create(LocalDate replicaActualityDate) {
            return create(replicaActualityDate, rowActualFromDt(replicaActualityDate), rowActualFromMonth(replicaActualityDate));
        }

        public History create(LocalDate replicaActualityDate, String... initValueColumnName) {
            return create(replicaActualityDate, rowActualFromDt(replicaActualityDate, initValueColumnName), rowActualFromMonth(replicaActualityDate, initValueColumnName));
        }

        public History create(LocalDate replicaActualityDate, HistColumn rowActualFromDt, HistColumn rowActualFromMonth) {
            return create(replicaActualityDate, rowActualFromDt, rowActualFromMonth, rowActualToDt(replicaActualityDate), rowActualToMonth(replicaActualityDate));
        }

        public History create(LocalDate replicaActualityDate,
                              HistColumn rowActualFromDt,
                              HistColumn rowActualFromMonth,
                              HistColumn rowActualToDt,
                              HistColumn rowActualToMonth) {
            addHstColumn(rowActualToDt);
            addHstColumn(ctlValidto(replicaActualityDate));
            addHstColumn(rowActualToMonth);

            addColumn(rowActualFromDt);
            addSnpColumn(rowActualFromMonth);
            addColumn(literal(lit(ctlLoadingId), "ctl_loading"));
            addColumn(ctlValidFrom(replicaActualityDate));
            addColumn(ctlAction());
            addColumn(hashColumn());
            return hst;
        }

        private HistColumn hashColumn() {
            return HistColumn
                    .histCol("row_hash")
                    .initValue(UpdateExpression::hashViaSha2, false)
                    .whenRowUpdated()
                    .updatedValue((prev, curr) -> UpdateExpression.hashViaSha2(curr))
                    .create();
        }

        private HistColumn ctlAction() {
            return HistColumn
                    .histCol("ctl_action")
                    .initValue(lit("I"))
                    .whenRowUpdated()
                    .updatedValue(lit("U"))
                    .staleValue((prev, curr) -> prev.col("ctl_action"))
                    .deletedValue(lit("D"))
                    .create();
        }

        private HistColumn ctlValidFrom(LocalDate replicaActualityDate) {
            return HistColumn.histCol("ctl_validfrom")
                    .initValue(lit(Timestamp.valueOf(replicaActualityDate.atStartOfDay())))
                    .whenRowUpdated()
                    .updatedValue(lit(Timestamp.valueOf(replicaActualityDate.atStartOfDay())))
                    .create();
        }

        private HistColumn ctlValidto(LocalDate replicaActualityDate) {
            final Column yesterday = lit(Timestamp.valueOf(replicaActualityDate.minusDays(1).atStartOfDay()));
            final Column year10k = lit(DateConstants.MAX_DATETIME);
            return HistColumn.histCol("ctl_validto")
                    .whenRowUpdated()
                    .updatedValue(year10k)
                    .staleValue(yesterday)
                    .create();
        }

        private HistColumn literal(Column literal, String colName) {
            return HistColumn
                    .histCol(colName)
                    .initValue(literal)
                    .whenRowUpdated()
                    .updatedValue(literal)
                    .create();
        }

        private HistColumn rowActualFromDt(LocalDate replicaActualityDate) {
            return rowActualFrom("row_actual_from_dt", lit(Timestamp.valueOf(replicaActualityDate.atStartOfDay())));
        }

        private HistColumn rowActualFromMonth(LocalDate replicaActualityDate) {
            Column rowActualFromDt = lit(Timestamp.valueOf(replicaActualityDate.atStartOfDay()));
            return rowActualFrom(ACTUAL_PARTITION_COLUMN, date_format(rowActualFromDt, MONTH_PART_FORMAT));
        }

        private HistColumn rowActualFrom(String histColumnName, Column column) {
            return HistColumn.histCol(histColumnName)
                    .whenRowUpdated()
                    .updatedValue(column)
                    .create();
        }

        private HistColumn rowActualFromDt(LocalDate replicaActualityDate, String... initValueColumnName) {
            Column rowActualFromDt = lit(Timestamp.valueOf(replicaActualityDate.atStartOfDay()));
            Function<Dataset<Row>, Column> initValue = datamart -> {
                Column[] columnNames = Stream.of(initValueColumnName).map(datamart::col).toArray(Column[]::new);
                Column coalesceDatamartColumns = coalesce(columnNames);
                return when(coalesceDatamartColumns.isNotNull(), coalesceDatamartColumns)
                        .otherwise(rowActualFromDt);
            };
            return rowActualFrom("row_actual_from_dt", rowActualFromDt, initValue);
        }

        private HistColumn rowActualFromMonth(LocalDate replicaActualityDate, String... initValueColumnName) {
            Column rowActualFromDt = lit(Timestamp.valueOf(replicaActualityDate.atStartOfDay()));
            //если все варианты бизнес-даты NULL, берем дату построения
            Function<Dataset<Row>, Column> initValue = datamart -> {
                Column[] columnNames = Stream.of(initValueColumnName).map(datamart::col).toArray(Column[]::new);
                Column coalesceDatamartColumns = coalesce(columnNames);
                return when(coalesceDatamartColumns.isNotNull(), date_format(coalesceDatamartColumns, MONTH_PART_FORMAT))
                        .otherwise(date_format(rowActualFromDt, MONTH_PART_FORMAT));
            };
            return rowActualFrom("row_actual_from_month", date_format(rowActualFromDt, MONTH_PART_FORMAT), initValue);
        }

        private HistColumn rowActualFrom(String histColumnName, Column replicaActualityDateTs, Function<Dataset<Row>, Column> initValue) {
            return HistColumn.histCol(histColumnName)
                    .initValue(initValue)
                    .whenRowUpdated()
                    .updatedValue(replicaActualityDateTs)
                    .create();
        }

        private HistColumn rowActualToDt(LocalDate replicaActualityDate) {
            final Column yesterday = lit(Timestamp.valueOf(replicaActualityDate.minusDays(1).atStartOfDay()));
            final Column year10k = lit(Timestamp.valueOf(DateConstants.MAX_DATETIME.toLocalDateTime()));
            return rowActualTo("row_actual_to_dt", yesterday, year10k);
        }

        private HistColumn rowActualToMonth(LocalDate replicaActualityDate) {
            final Column yesterday = lit(Timestamp.valueOf(replicaActualityDate.minusDays(1).atStartOfDay()));
            final Column year10k = lit(Timestamp.valueOf(DateConstants.MAX_DATETIME.toLocalDateTime()));
            return rowActualTo(HISTORY_PARTITION_COLUMN, date_format(yesterday, MONTH_PART_FORMAT), date_format(year10k, MONTH_PART_FORMAT));
        }

        private HistColumn rowActualTo(String histColumnName, Column yesterday, Column year10k) {
            return HistColumn.histCol(histColumnName)
                    .whenRowUpdated()
                    .updatedValue(year10k)
                    .staleValue(yesterday)
                    .create();
        }

        public HistBuilder setIncremental() {
            hst.incremental = true;
            return this;
        }

        public HistBuilder setIncremental(Function<Dataset<Row>, Column> deleteExpression) {
            hst.incremental = true;
            hst.deleteExpression = deleteExpression;
            return this;
        }
    }


    private static class AdditionalTable {

        private final Dataset<Row> table;

        private final Function<Dataset<Row>, Column> joinExpr;

        private final String joinType;

        private AdditionalTable(Dataset<Row> table, Function<Dataset<Row>, Column> joinExpr, String joinType) {
            this.table = table;
            this.joinExpr = joinExpr;
            this.joinType = joinType;
        }

        public Dataset<Row> table() {
            return table;
        }

        public Column joinExpr(Dataset<Row> frame) {
            return joinExpr.apply(frame);
        }

        public String joinType() {
            return joinType;
        }
    }
}

