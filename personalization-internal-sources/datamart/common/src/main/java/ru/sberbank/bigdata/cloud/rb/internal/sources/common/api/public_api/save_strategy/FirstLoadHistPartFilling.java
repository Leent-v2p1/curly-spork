package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_strategy;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.QueryExecution;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.hive.SchemaGrantsChecker;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.DatamartNaming;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.save.TableSaveHelper;

import java.time.LocalDate;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.not;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.FieldConstants.MONTH_PART_FORMAT;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.date.DateHelper.startOfYesterday;

/**
 * Класс позволяет при первом построении витрины наполнить историю витрины некоторыми значениями.
 * Какие строки попадают в снп - определяет параметр {@link #actualCondition}, остальное пойдет в историю.
 * В исторической части заменится колонка 'row_actual_to_dt' и добавится колонка #TableSaveHelper.HISTORY_PARTITION_COLUMN
 */
public class FirstLoadHistPartFilling extends SeparateHistoryTableSavingStrategy {
    private final Column actualCondition;
    private final Column histRowActualToDt;
    private final LocalDate buildDate;
    private boolean isFirstLoading;

    public FirstLoadHistPartFilling(Column actualCondition, Column histRowActualToDt, boolean isFirstLoading, LocalDate buildDate) {
        super();
        this.actualCondition = actualCondition;
        this.histRowActualToDt = histRowActualToDt;
        this.isFirstLoading = isFirstLoading;
        this.buildDate = buildDate;
    }

    @Override
    public void save(SparkSession context, Dataset<Row> datamart, DatamartNaming naming, SchemaGrantsChecker schemaGrantsChecker) {
        super.save(context, datamart.where(actualCondition), naming, schemaGrantsChecker);
        if (isFirstLoading) {
            log.info("first loading. Saving hist part of snapshot data to hist datamart based on condition 'not({})'", actualCondition);
            TableSaveHelper saveHelper = new TableSaveHelper(context);
            final Dataset<Row> histDatamart = datamart
                    .drop(ACTUAL_PARTITION_COLUMN)
                    .withColumn("row_actual_to_dt", histRowActualToDt)
                    .withColumn(HISTORY_PARTITION_COLUMN, date_format(col("row_actual_to_dt"), MONTH_PART_FORMAT))
                    .withColumn("ctl_validto", lit(startOfYesterday(buildDate)))
                    .where(not(actualCondition));

            final String historyReserveName = naming.historyReserveFullTableName();
            final QueryExecution executionPlan = histDatamart.queryExecution();
            log.info("history part {} queryExecution: {}", historyReserveName, executionPlan);
            saveHelper.insertIntoTable(historyReserveName, histDatamart, HISTORY_PARTITIONING);
        }
    }
}
