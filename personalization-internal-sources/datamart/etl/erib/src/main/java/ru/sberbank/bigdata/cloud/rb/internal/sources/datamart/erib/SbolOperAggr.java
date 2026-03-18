package ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.erib;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.DatamartRef;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.PartialReplace;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.DatamartServiceFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.Datamart;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_remover.UpdatedPartitionRemover;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.DataFrameUtils;

import java.time.LocalDate;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.AutoConfigDatamartRunner.runner;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.DecimalTypes.DECIMAL_38_15;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.FieldConstants.DAY_PART;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.FieldConstants.MONTH_PART;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId.LAST_LOAD_START;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.date.DateHelper.generateMonthsDataset;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.SparkSQLUtil.isEmpty;

@DatamartRef(id = "custom_rb_sbol.sbol_oper_aggr", name = "Агрегаты по операциям СБОЛ")
@PartialReplace(partitioning = MONTH_PART, saveRemover = UpdatedPartitionRemover.class)
public class SbolOperAggr extends Datamart {

    private static final Logger log = LoggerFactory.getLogger(SbolOperAggr.class);
    static final StructType SBOL_OPER_AGGR_SCHEMA = new StructType()
            .add("epk_id", LongType)
            .add("month_part", StringType)
            .add("login_type", StringType)
            .add("exec_rub_total", DECIMAL_38_15)
            .add("exec_rub_web", DECIMAL_38_15)
            .add("exec_rub_mobil", DECIMAL_38_15)
            .add("exec_rub_atm", DECIMAL_38_15)
            .add("exec_rub_mb", DECIMAL_38_15)
            .add("exec_rub_other", DECIMAL_38_15)
            .add("exec_qty", LongType)
            .add("exec_web_qty", LongType)
            .add("exec_mobil_qty", LongType)
            .add("exec_atm_qty", LongType)
            .add("exec_mb_qty", LongType)
            .add("exec_other_qty", LongType)
            .add("fst_exec_dt", StringType)
            .add("lst_exec_dt", StringType);
    private static final String CSA = "CSA";
    private static final String MAPI = "MAPI";
    private static final String TERMINAL = "TERMINAL";
    private static final String MB = "MB";
    public static final Object[] LOGIN_TYPES = {"CSA", "MAPI", "TERMINAL", "MB"};
    private String lastLoadStart;

    @Override
    public void addDatafixes() {
        dataFixApi.castColumnIfNotCasted("epk_id", LongType);
    }

    @Override
    public void init(DatamartServiceFactory datamartServiceFactory) {
        if (!isFirstLoading) {
            this.lastLoadStart = datamartServiceFactory.parametersService().getLastStatistic(LAST_LOAD_START, statistic -> statistic.value);
        }
    }

    @Override
    public Dataset<Row> buildDatamart() {
        final String firstDayOfMonth = buildDate().withDayOfMonth(1).toString();

        final Dataset<Row> sbolOper = targetTable("sbol_oper");

        Column filter = col("epk_id").isNotNull()
                .and(col("oper_state_code").equalTo("EXECUTED"))
                .and(col("oper_amt").gt(0))
                .and(col(DAY_PART).lt(firstDayOfMonth));

        if (!isFirstLoading) {
            filter = filter.and(col(DAY_PART).geq(lastLoadStart));
        }

        final Dataset<Row> filteredSbolOper = saveTemp(sbolOper.where(filter), "filteredSbolOper");

        if (!isFirstLoading && isEmpty(filteredSbolOper)) {
            log.warn("Filtered dataframe is empty. Default statistics disabled. Build is stopped.");
            disableDefaultStatistics();
            return DataFrameUtils.createDF(dc.context(), SBOL_OPER_AGGR_SCHEMA);
        }

        final Dataset<Row> operAggrMonth = filteredSbolOper
                .withColumn("epk_id", coalesce(filteredSbolOper.col("epk_id").cast(LongType), lit(-1L)))
                .withColumn("login_type", coalesce(filteredSbolOper.col("login_type"), lit("")))
                .groupBy(col("epk_id"),
                        substring(col("date_create"), 1, 7).as("month"),
                        col("login_type").as("login_type"))
                .agg(
                        min(col("date_create")).as("fst_exec_dt"),
                        max(col("date_create")).as("lst_exec_dt"),
                        sum(col("oper_rur_amt")).as("exec_rub_total"),
                        count("*").as("exec_qty"),
                        sum(when(col("login_type").equalTo(CSA), col("oper_rur_amt"))
                                .otherwise(0)).as("exec_rub_web"),
                        sum(when(col("login_type").equalTo(MAPI), col("oper_rur_amt"))
                                .otherwise(0)).as("exec_rub_mobil"),
                        sum(when(col("login_type").equalTo(TERMINAL), col("oper_rur_amt"))
                                .otherwise(0)).as("exec_rub_atm"),
                        sum(when(col("login_type").equalTo(MB), col("oper_rur_amt"))
                                .otherwise(0)).as("exec_rub_mb"),
                        sum(when(not(col("login_type").isin(LOGIN_TYPES)), col("oper_rur_amt"))
                                .otherwise(0)).as("exec_rub_other"),
                        sum(when(col("login_type").equalTo(CSA), 1)
                                .otherwise(0)).as("exec_web_qty"),
                        sum(when(col("login_type").equalTo(MAPI), 1)
                                .otherwise(0)).as("exec_mobil_qty"),
                        sum(when(col("login_type").equalTo(TERMINAL), 1)
                                .otherwise(0)).as("exec_atm_qty"),
                        sum(when(col("login_type").equalTo(MB), 1)
                                .otherwise(0)).as("exec_mb_qty"),
                        sum(when(not(col("login_type").isin(LOGIN_TYPES)), 1)
                                .otherwise(0)).as("exec_other_qty")
                );

        Dataset<Row> result;

        if (isFirstLoading) {
            final Dataset<Row> minMonth = operAggrMonth
                    .groupBy(
                            col("epk_id")
                    )
                    .agg(
                            min("fst_exec_dt").as("min_fst_exec_dt")
                    );

            final Dataset<Row> generatedMonths = generateMonthsDataset(
                    dc.context(),
                    LocalDate.of(2015, 1, 1),
                    buildDate().minusMonths(1)
            );

            final Dataset<Row> monthsByClients = minMonth
                    .join(broadcast(generatedMonths), generatedMonths.col("month").geq(substring(minMonth.col("min_fst_exec_dt"), 1, 7)))
                    .select(
                            minMonth.col("epk_id"),
                            generatedMonths.col("month")
                    );

            result = monthsByClients.join(operAggrMonth, operAggrMonth.col("month").equalTo(monthsByClients.col("month"))
                            .and(operAggrMonth.col("epk_id").equalTo(monthsByClients.col("epk_id"))), "left")
                    .select(
                            monthsByClients.col("epk_id"),
                            monthsByClients.col("month").as(MONTH_PART),
                            operAggrMonth.col("login_type"),
                            coalesce(operAggrMonth.col("exec_rub_total"), lit(0)).cast(DECIMAL_38_15).as("exec_rub_total"),
                            coalesce(operAggrMonth.col("exec_rub_web"),lit(0)).cast(DECIMAL_38_15).as("exec_rub_web"),
                            coalesce(operAggrMonth.col("exec_rub_mobil"),lit(0)).cast(DECIMAL_38_15).as("exec_rub_mobil"),
                            coalesce(operAggrMonth.col("exec_rub_atm"),lit(0)).cast(DECIMAL_38_15).as("exec_rub_atm"),
                            coalesce(operAggrMonth.col("exec_rub_mb"),lit(0)).cast(DECIMAL_38_15).as("exec_rub_mb"),
                            coalesce(operAggrMonth.col("exec_rub_other"),lit(0)).cast(DECIMAL_38_15).as("exec_rub_other"),
                            coalesce(operAggrMonth.col("exec_qty"),lit(0)).cast(LongType).as("exec_qty"),
                            coalesce(operAggrMonth.col("exec_web_qty"),lit(0)).cast(LongType).as("exec_web_qty"),
                            coalesce(operAggrMonth.col("exec_mobil_qty"),lit(0)).cast(LongType).as("exec_mobil_qty"),
                            coalesce(operAggrMonth.col("exec_atm_qty"),lit(0)).cast(LongType).as("exec_atm_qty"),
                            coalesce(operAggrMonth.col("exec_mb_qty"),lit(0)).cast(LongType).as("exec_mb_qty"),
                            coalesce(operAggrMonth.col("exec_other_qty"),lit(0)).cast(LongType).as("exec_other_qty"),
                            operAggrMonth.col("fst_exec_dt"),
                            operAggrMonth.col("lst_exec_dt")
                    );
        } else {
            final Dataset<Row> dmAggr = self()
                    .where(col(MONTH_PART).equalTo(substring(add_months(lit(lastLoadStart), -1), 1, 7)))
                    .select(
                            col("epk_id"),
                            coalesce(col("login_type"), lit("")).as("login_type"),
                            col(MONTH_PART)
                    );

            result = operAggrMonth.join(dmAggr, dmAggr.col("epk_id").equalTo(operAggrMonth.col("epk_id"))
                            .and(dmAggr.col("login_type").equalTo(operAggrMonth.col("login_type"))), "full")
                    .withColumn("login_type_without_null", coalesce(operAggrMonth.col("login_type"), dmAggr.col("login_type")))
                    .select(
                            coalesce(operAggrMonth.col("epk_id"), dmAggr.col("epk_id")).as("epk_id"),
                            when(col("login_type_without_null").equalTo(""), null)
                                    .otherwise(col("login_type_without_null"))
                                    .cast(StringType)
                                    .as("login_type"),
                            coalesce(operAggrMonth.col("month"), lit(lastLoadStart.substring(0, 7))).as(MONTH_PART),
                            coalesce(operAggrMonth.col("exec_rub_total"), lit(0)).cast(DECIMAL_38_15).as("exec_rub_total"),
                            coalesce(operAggrMonth.col("exec_rub_web"),lit(0)).cast(DECIMAL_38_15).as("exec_rub_web"),
                            coalesce(operAggrMonth.col("exec_rub_mobil"),lit(0)).cast(DECIMAL_38_15).as("exec_rub_mobil"),
                            coalesce(operAggrMonth.col("exec_rub_atm"),lit(0)).cast(DECIMAL_38_15).as("exec_rub_atm"),
                            coalesce(operAggrMonth.col("exec_rub_mb"),lit(0)).cast(DECIMAL_38_15).as("exec_rub_mb"),
                            coalesce(operAggrMonth.col("exec_rub_other"),lit(0)).cast(DECIMAL_38_15).as("exec_rub_other"),
                            coalesce(operAggrMonth.col("exec_qty"),lit(0)).cast(LongType).as("exec_qty"),
                            coalesce(operAggrMonth.col("exec_web_qty"),lit(0)).cast(LongType).as("exec_web_qty"),
                            coalesce(operAggrMonth.col("exec_mobil_qty"),lit(0)).cast(LongType).as("exec_mobil_qty"),
                            coalesce(operAggrMonth.col("exec_atm_qty"),lit(0)).cast(LongType).as("exec_atm_qty"),
                            coalesce(operAggrMonth.col("exec_mb_qty"),lit(0)).cast(LongType).as("exec_mb_qty"),
                            coalesce(operAggrMonth.col("exec_other_qty"),lit(0)).cast(LongType).as("exec_other_qty"),
                            operAggrMonth.col("fst_exec_dt"),
                            operAggrMonth.col("lst_exec_dt")
                    );
        }
        addStatistic(LAST_LOAD_START, firstDayOfMonth);

        return result;
    }

    public static void main(String[] args) {
        runner().run(SbolOperAggr.class);
    }
}