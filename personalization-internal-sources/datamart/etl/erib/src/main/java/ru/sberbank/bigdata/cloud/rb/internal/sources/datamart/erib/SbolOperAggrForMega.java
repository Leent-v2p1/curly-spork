package ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.erib;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.DatamartRef;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.PartialReplace;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.DatamartServiceFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.Datamart;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_remover.UpdatedPartitionRemover;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.DataFrameUtils;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.*;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.AutoConfigDatamartRunner.runner;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.DecimalTypes.DECIMAL_38_15;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.FieldConstants.MONTH_PART;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId.LAST_LOAD_START;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.SparkSQLUtil.isEmpty;

@DatamartRef(id = "custom_rb_sbol.sbol_oper_aggr_for_mega", name = "Агрегат по операциям СБОЛ для Мегавитрины")
@PartialReplace(partitioning = MONTH_PART, saveRemover = UpdatedPartitionRemover.class)
public class SbolOperAggrForMega extends Datamart {

    public static final StructType SBOL_OPER_AGGR_FOR_MEGA_SCHEMA = new StructType()
            .add("epk_id", LongType)
            .add("month_part", StringType)
            .add("sbol_fst_txn_dt", StringType)
            .add("sbol_lst_txn_dt", StringType)
            .add("sbol_fst_txn_ever_dt", StringType)
            .add("sbol_lst_txn_ever_dt", StringType)
            .add("sbol_txn_1m_amt", DECIMAL_38_15)
            .add("sbol_txn_1m_qty", LongType)
            .add("sbol_txn_3m_amt", DECIMAL_38_15)
            .add("sbol_txn_3m_qty", LongType)
            .add("sbol_txn_atm_1m_amt", DECIMAL_38_15)
            .add("sbol_txn_atm_1m_qty", LongType)
            .add("sbol_txn_atm_3m_amt", DECIMAL_38_15)
            .add("sbol_txn_atm_3m_qty", LongType)
            .add("sbol_txn_mob_1m_amt", DECIMAL_38_15)
            .add("sbol_txn_mob_1m_qty", LongType)
            .add("sbol_txn_mob_3m_amt", DECIMAL_38_15)
            .add("sbol_txn_mob_3m_qty", LongType)
            .add("sbol_txn_web_1m_amt", DECIMAL_38_15)
            .add("sbol_txn_web_1m_qty", LongType)
            .add("sbol_txn_web_3m_amt", DECIMAL_38_15)
            .add("sbol_txn_web_3m_qty", LongType)
            .add("sbol_txn_main_1m_amt", DECIMAL_38_15)
            .add("sbol_txn_main_1m_qty", LongType)
            .add("sbol_txn_main_3m_amt", DECIMAL_38_15)
            .add("sbol_txn_main_3m_qty", LongType)
            .add("sbol_txn_mb_1m_amt", DECIMAL_38_15)
            .add("sbol_txn_mb_1m_qty", LongType)
            .add("sbol_txn_mb_3m_amt", DECIMAL_38_15)
            .add("sbol_txn_mb_3m_qty", LongType)
            .add("sbol_txn_other_1m_amt", DECIMAL_38_15)
            .add("sbol_txn_other_1m_qty", LongType)
            .add("sbol_txn_other_3m_amt", DECIMAL_38_15)
            .add("sbol_txn_other_3m_qty", LongType)
            .add("sbol_mnth_fst_dt_qty", DoubleType)
            .add("sbol_mnth_lst_dt_qty", DoubleType);
    private static final String MAX_DATE = "2099-01-01";
    private static final String MIN_DATE = "1900-01-01";
    private final Logger log = LoggerFactory.getLogger(SbolOperAggrForMega.class);
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
        final Dataset<Row> sbolOperAggr = targetTable("sbol_oper_aggr");
        final String currentMonth = buildDate().toString().substring(0, 7);

        Column filter = col(MONTH_PART).lt(currentMonth);

        if (!isFirstLoading) {
            filter = filter.and(col(MONTH_PART).geq((substring(add_months(lit(lastLoadStart), -2), 1, 7))));
        }

        final Dataset<Row> operAggrByClient = sbolOperAggr
                .where(filter)
                .withColumn("epk_id", coalesce(sbolOperAggr.col("epk_id").cast(LongType), lit(-1L)))
                .groupBy(
                        col("epk_id"),
                        col(MONTH_PART)
                )
                .agg(
                        min(col("fst_exec_dt")).as("fst_exec_dt"),
                        max(col("lst_exec_dt")).as("lst_exec_dt"),
                        sum(col("exec_rub_total")).as("exec_rub_total"),
                        sum(col("exec_qty")).as("exec_qty"),
                        sum(col("exec_rub_atm")).as("exec_rub_atm"),
                        sum(col("exec_atm_qty")).as("exec_atm_qty"),
                        sum(col("exec_rub_mobil")).as("exec_rub_mobil"),
                        sum(col("exec_mobil_qty")).as("exec_mobil_qty"),
                        sum(col("exec_rub_web")).as("exec_rub_web"),
                        sum(col("exec_web_qty")).as("exec_web_qty"),
                        sum(col("exec_rub_atm").plus(col("exec_rub_mobil")).plus(col("exec_rub_web"))).as("exec_rub_main"),
                        sum(col("exec_atm_qty").plus(col("exec_mobil_qty")).plus(col("exec_web_qty"))).as("exec_main_qty"),
                        sum(col("exec_rub_mb")).as("exec_rub_mb"),
                        sum(col("exec_mb_qty")).as("exec_mb_qty"),
                        sum(col("exec_rub_other")).as("exec_rub_other"),
                        sum(col("exec_other_qty")).as("exec_other_qty")
                );

        final WindowSpec unboundedPrecedingWindow = Window.partitionBy(col("epk_id"))
                .orderBy(col(MONTH_PART))
                .rowsBetween(Window.unboundedPreceding(), Window.currentRow());

        final WindowSpec twoPrecedingWindow = Window.partitionBy(col("epk_id"))
                .orderBy(col(MONTH_PART))
                .rowsBetween(Window.currentRow() - 2, Window.currentRow());

        Dataset<Row> result;

        if (isFirstLoading) {

            final Dataset<Row> operAggrSumQty = operAggrByClient
                    .select(
                            col("epk_id"),
                            col(MONTH_PART),
                            col("fst_exec_dt").as("sbol_fst_txn_dt"),
                            col("lst_exec_dt").as("sbol_lst_txn_dt"),
                            min("fst_exec_dt").over(unboundedPrecedingWindow).as("sbol_fst_txn_ever_dt"),
                            max("lst_exec_dt").over(unboundedPrecedingWindow).as("sbol_lst_txn_ever_dt"),
                            col("exec_rub_total").as("sbol_txn_1m_amt"),
                            col("exec_qty").as("sbol_txn_1m_qty"),
                            sum(col("exec_rub_total")).over(twoPrecedingWindow).as("sbol_txn_3m_amt"),
                            sum(col("exec_qty")).over(twoPrecedingWindow).as("sbol_txn_3m_qty"),
                            col("exec_rub_atm").as("sbol_txn_atm_1m_amt"),
                            col("exec_atm_qty").as("sbol_txn_atm_1m_qty"),
                            sum(col("exec_rub_atm")).over(twoPrecedingWindow).as("sbol_txn_atm_3m_amt"),
                            sum(col("exec_atm_qty")).over(twoPrecedingWindow).as("sbol_txn_atm_3m_qty"),
                            col("exec_rub_mobil").as("sbol_txn_mob_1m_amt"),
                            col("exec_mobil_qty").as("sbol_txn_mob_1m_qty"),
                            sum(col("exec_rub_mobil")).over(twoPrecedingWindow).as("sbol_txn_mob_3m_amt"),
                            sum(col("exec_mobil_qty")).over(twoPrecedingWindow).as("sbol_txn_mob_3m_qty"),
                            col("exec_rub_web").as("sbol_txn_web_1m_amt"),
                            col("exec_web_qty").as("sbol_txn_web_1m_qty"),
                            sum(col("exec_rub_web")).over(twoPrecedingWindow).as("sbol_txn_web_3m_amt"),
                            sum(col("exec_web_qty")).over(twoPrecedingWindow).as("sbol_txn_web_3m_qty"),
                            col("exec_rub_main").as("sbol_txn_main_1m_amt").cast(DECIMAL_38_15),
                            col("exec_main_qty").as("sbol_txn_main_1m_qty"),
                            sum(col("exec_rub_main")).over(twoPrecedingWindow).as("sbol_txn_main_3m_amt").cast(DECIMAL_38_15),
                            sum(col("exec_main_qty")).over(twoPrecedingWindow).as("sbol_txn_main_3m_qty"),
                            col("exec_rub_mb").as("sbol_txn_mb_1m_amt"),
                            col("exec_mb_qty").as("sbol_txn_mb_1m_qty"),
                            sum(col("exec_rub_mb")).over(twoPrecedingWindow).as("sbol_txn_mb_3m_amt"),
                            sum(col("exec_mb_qty")).over(twoPrecedingWindow).as("sbol_txn_mb_3m_qty"),
                            col("exec_rub_other").as("sbol_txn_other_1m_amt"),
                            col("exec_other_qty").as("sbol_txn_other_1m_qty"),
                            sum(col("exec_rub_other")).over(twoPrecedingWindow).as("sbol_txn_other_3m_amt"),
                            sum(col("exec_other_qty")).over(twoPrecedingWindow).as("sbol_txn_other_3m_qty")
                    );

            result = operAggrSumQty
                    .select(
                            col("*"),
                            monthsCount("sbol_fst_txn_ever_dt").as("sbol_mnth_fst_dt_qty"),
                            monthsCount("sbol_lst_txn_ever_dt").as("sbol_mnth_lst_dt_qty")
                    );
        } else {

            final Dataset<Row> sbolOperAggrForCount = saveTemp(sbolOperAggr
                    .where(col(MONTH_PART).lt(currentMonth).and(col(MONTH_PART).geq(substring(lit(lastLoadStart), 1, 7)))), "sbolOperAggrForCount");

            if (isEmpty(sbolOperAggrForCount)) {
                log.warn("Filtered dataframe is empty. Default statistics disabled. Build is stopped.");
                disableDefaultStatistics();
                return DataFrameUtils.createDF(dc.context(), SBOL_OPER_AGGR_FOR_MEGA_SCHEMA);
            }

            final Dataset<Row> operAggrSumQty = operAggrByClient
                    .select(
                            col("epk_id"),
                            col(MONTH_PART),
                            col("fst_exec_dt"),
                            col("lst_exec_dt"),
                            col("exec_rub_total").as("sbol_txn_1m_amt"),
                            col("exec_qty").as("sbol_txn_1m_qty"),
                            sum(col("exec_rub_total")).over(twoPrecedingWindow).as("sbol_txn_3m_amt"),
                            sum(col("exec_qty")).over(twoPrecedingWindow).as("sbol_txn_3m_qty"),
                            col("exec_rub_atm").as("sbol_txn_atm_1m_amt"),
                            col("exec_atm_qty").as("sbol_txn_atm_1m_qty"),
                            sum(col("exec_rub_atm")).over(twoPrecedingWindow).as("sbol_txn_atm_3m_amt"),
                            sum(col("exec_atm_qty")).over(twoPrecedingWindow).as("sbol_txn_atm_3m_qty"),
                            col("exec_rub_mobil").as("sbol_txn_mob_1m_amt"),
                            col("exec_mobil_qty").as("sbol_txn_mob_1m_qty"),
                            sum(col("exec_rub_mobil")).over(twoPrecedingWindow).as("sbol_txn_mob_3m_amt"),
                            sum(col("exec_mobil_qty")).over(twoPrecedingWindow).as("sbol_txn_mob_3m_qty"),
                            col("exec_rub_web").as("sbol_txn_web_1m_amt"),
                            col("exec_web_qty").as("sbol_txn_web_1m_qty"),
                            sum(col("exec_rub_web")).over(twoPrecedingWindow).as("sbol_txn_web_3m_amt"),
                            sum(col("exec_web_qty")).over(twoPrecedingWindow).as("sbol_txn_web_3m_qty"),
                            col("exec_rub_main").as("sbol_txn_main_1m_amt"),
                            col("exec_main_qty").as("sbol_txn_main_1m_qty"),
                            sum(col("exec_rub_main")).over(twoPrecedingWindow).as("sbol_txn_main_3m_amt"),
                            sum(col("exec_main_qty")).over(twoPrecedingWindow).as("sbol_txn_main_3m_qty"),
                            col("exec_rub_mb").as("sbol_txn_mb_1m_amt"),
                            col("exec_mb_qty").as("sbol_txn_mb_1m_qty"),
                            sum(col("exec_rub_mb")).over(twoPrecedingWindow).as("sbol_txn_mb_3m_amt"),
                            sum(col("exec_mb_qty")).over(twoPrecedingWindow).as("sbol_txn_mb_3m_qty"),
                            col("exec_rub_other").as("sbol_txn_other_1m_amt"),
                            col("exec_other_qty").as("sbol_txn_other_1m_qty"),
                            sum(col("exec_rub_other")).over(twoPrecedingWindow).as("sbol_txn_other_3m_amt"),
                            sum(col("exec_other_qty")).over(twoPrecedingWindow).as("sbol_txn_other_3m_qty")
                    );

            final Dataset<Row> newOperMega = operAggrSumQty
                    .where(col(MONTH_PART).geq(lastLoadStart.substring(0, 7)))
                    .select(col("*"));

            final Dataset<Row> oldOperMegaDm = self()
                    .where(col(MONTH_PART).equalTo(substring(add_months(lit(lastLoadStart), -1), 1, 7)))
                    .select(
                            coalesce(col("epk_id").cast(LongType), lit(-1L)).as("epk_id"),
                            col(MONTH_PART),
                            col("sbol_fst_txn_ever_dt"),
                            col("sbol_lst_txn_ever_dt")
                    );

            final Dataset<Row> updateEverData = oldOperMegaDm
                    .join(newOperMega, newOperMega.col("epk_id").equalTo(oldOperMegaDm.col("epk_id")), "full")
                    .select(
                            coalesce(newOperMega.col("epk_id"), oldOperMegaDm.col("epk_id")).as("epk_id"),
                            coalesce(newOperMega.col(MONTH_PART), lit(lastLoadStart.substring(0, 7))).as(MONTH_PART),
                            newOperMega.col("fst_exec_dt").as("sbol_fst_txn_dt"),
                            newOperMega.col("lst_exec_dt").as("sbol_lst_txn_dt"),
                            when(coalesce(oldOperMegaDm.col("sbol_fst_txn_ever_dt"), lit(MAX_DATE))
                                            .gt(coalesce(newOperMega.col("fst_exec_dt"), lit(MAX_DATE))),
                                    coalesce(newOperMega.col("fst_exec_dt"), lit(MAX_DATE)))
                                    .otherwise(coalesce(oldOperMegaDm.col("sbol_fst_txn_ever_dt"), lit(MAX_DATE)))
                                    .as("sbol_fst_txn_ever_dt"),
                            when(coalesce(oldOperMegaDm.col("sbol_lst_txn_ever_dt"), lit(MIN_DATE))
                                            .gt(coalesce(newOperMega.col("fst_exec_dt"), lit(MIN_DATE))),
                                    coalesce(oldOperMegaDm.col("sbol_lst_txn_ever_dt"), lit(MIN_DATE)))
                                    .otherwise(coalesce(newOperMega.col("lst_exec_dt"), lit(MIN_DATE)))
                                    .as("sbol_lst_txn_ever_dt"),
                            newOperMega.col("sbol_txn_1m_amt").as("sbol_txn_1m_amt"),
                            newOperMega.col("sbol_txn_1m_qty").as("sbol_txn_1m_qty"),
                            newOperMega.col("sbol_txn_3m_amt").as("sbol_txn_3m_amt"),
                            newOperMega.col("sbol_txn_3m_qty").as("sbol_txn_3m_qty"),
                            newOperMega.col("sbol_txn_atm_1m_amt").as("sbol_txn_atm_1m_amt"),
                            newOperMega.col("sbol_txn_atm_1m_qty").as("sbol_txn_atm_1m_qty"),
                            newOperMega.col("sbol_txn_atm_3m_amt").as("sbol_txn_atm_3m_amt"),
                            newOperMega.col("sbol_txn_atm_3m_qty").as("sbol_txn_atm_3m_qty"),
                            newOperMega.col("sbol_txn_mob_1m_amt").as("sbol_txn_mob_1m_amt"),
                            newOperMega.col("sbol_txn_mob_1m_qty").as("sbol_txn_mob_1m_qty"),
                            newOperMega.col("sbol_txn_mob_3m_amt").as("sbol_txn_mob_3m_amt"),
                            newOperMega.col("sbol_txn_mob_3m_qty").as("sbol_txn_mob_3m_qty"),
                            newOperMega.col("sbol_txn_web_1m_amt").as("sbol_txn_web_1m_amt"),
                            newOperMega.col("sbol_txn_web_1m_qty").as("sbol_txn_web_1m_qty"),
                            newOperMega.col("sbol_txn_web_3m_amt").as("sbol_txn_web_3m_amt"),
                            newOperMega.col("sbol_txn_web_3m_qty").as("sbol_txn_web_3m_qty"),
                            newOperMega.col("sbol_txn_main_1m_amt").as("sbol_txn_main_1m_amt"),
                            newOperMega.col("sbol_txn_main_1m_qty").as("sbol_txn_main_1m_qty"),
                            newOperMega.col("sbol_txn_main_3m_amt").as("sbol_txn_main_3m_amt"),
                            newOperMega.col("sbol_txn_main_3m_qty").as("sbol_txn_main_3m_qty"),
                            newOperMega.col("sbol_txn_mb_1m_amt").as("sbol_txn_mb_1m_amt"),
                            newOperMega.col("sbol_txn_mb_1m_qty").as("sbol_txn_mb_1m_qty"),
                            newOperMega.col("sbol_txn_mb_3m_amt").as("sbol_txn_mb_3m_amt"),
                            newOperMega.col("sbol_txn_mb_3m_qty").as("sbol_txn_mb_3m_qty"),
                            newOperMega.col("sbol_txn_other_1m_amt").as("sbol_txn_other_1m_amt"),
                            newOperMega.col("sbol_txn_other_1m_qty").as("sbol_txn_other_1m_qty"),
                            newOperMega.col("sbol_txn_other_3m_amt").as("sbol_txn_other_3m_amt"),
                            newOperMega.col("sbol_txn_other_3m_qty").as("sbol_txn_other_3m_qty")
                    );

            result = updateEverData
                    .select(
                            col("epk_id"),
                            col(MONTH_PART),
                            col("sbol_fst_txn_dt"),
                            col("sbol_lst_txn_dt"),
                            when(col("sbol_fst_txn_ever_dt").equalTo(lit(MAX_DATE)), null)
                                    .otherwise(col("sbol_fst_txn_ever_dt")).as("sbol_fst_txn_ever_dt"),
                            when(col("sbol_lst_txn_ever_dt").equalTo(lit(MIN_DATE)), null)
                                    .otherwise(col("sbol_lst_txn_ever_dt")).as("sbol_lst_txn_ever_dt"),
                            col("sbol_txn_1m_amt"),
                            col("sbol_txn_1m_qty"),
                            col("sbol_txn_3m_amt"),
                            col("sbol_txn_3m_qty"),
                            col("sbol_txn_atm_1m_amt"),
                            col("sbol_txn_atm_1m_qty"),
                            col("sbol_txn_atm_3m_amt"),
                            col("sbol_txn_atm_3m_qty"),
                            col("sbol_txn_mob_1m_amt"),
                            col("sbol_txn_mob_1m_qty"),
                            col("sbol_txn_mob_3m_amt"),
                            col("sbol_txn_mob_3m_qty"),
                            col("sbol_txn_web_1m_amt"),
                            col("sbol_txn_web_1m_qty"),
                            col("sbol_txn_web_3m_amt"),
                            col("sbol_txn_web_3m_qty"),
                            col("sbol_txn_main_1m_amt").cast(DECIMAL_38_15),
                            col("sbol_txn_main_1m_qty"),
                            col("sbol_txn_main_3m_amt").cast(DECIMAL_38_15),
                            col("sbol_txn_main_3m_qty"),
                            col("sbol_txn_mb_1m_amt"),
                            col("sbol_txn_mb_1m_qty"),
                            col("sbol_txn_mb_3m_amt"),
                            col("sbol_txn_mb_3m_qty"),
                            col("sbol_txn_other_1m_amt"),
                            col("sbol_txn_other_1m_qty"),
                            col("sbol_txn_other_3m_amt"),
                            col("sbol_txn_other_3m_qty"),
                            when(col("sbol_fst_txn_ever_dt").equalTo(MAX_DATE), null)
                                    .otherwise(monthsCount("sbol_fst_txn_ever_dt")).as("sbol_mnth_fst_dt_qty"),
                            when(col("sbol_lst_txn_ever_dt").equalTo(MIN_DATE), null)
                                    .otherwise(monthsCount("sbol_lst_txn_ever_dt")).as("sbol_mnth_lst_dt_qty")
                    );
        }

        addStatistic(LAST_LOAD_START, buildDate().toString());

        return result;
    }

    private Column monthsCount(String colName) {
        return months_between(date_add(add_months(concat(col(MONTH_PART), lit("-01")), 1), -1), col(colName));
    }

    public static void main(String[] args) {
        runner().run(SbolOperAggrForMega.class);
    }
}
