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
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.DataFrameUtils;

import java.time.LocalDate;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.TimestampType;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.AutoConfigDatamartRunner.runner;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.FieldConstants.DAY_PART;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.FieldConstants.MONTH_PART;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId.LAST_LOAD_START;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.date.DateHelper.generateMonthsDataset;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.SparkSQLUtil.isEmpty;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.erib.SbolConstants.*;

/**
 * Агрегаты по логинам СБОЛ по клиенту
 */
@DatamartRef(id = "custom_rb_sbol.sbol_logon_aggr", name = "Агрегаты по логинам СБОЛ по клиенту")
@PartialReplace(partitioning = MONTH_PART)
public class SbolLogonAggregate extends Datamart {

    private static final Logger log = LoggerFactory.getLogger(SbolLogonAggregate.class);
    static final StructType SBOL_LOGON_AGGR_SCHEMA = new StructType()
            .add("epk_id", LongType)
            .add("month_part", StringType)
            .add("fst_logon_dt", TimestampType)
            .add("lst_logon_dt", TimestampType)
            .add("tot_qty", LongType)
            .add("fst_mobil_dt", TimestampType)
            .add("lst_mobil_dt", TimestampType)
            .add("mobil_qty", LongType)
            .add("fst_web_dt", TimestampType)
            .add("lst_web_dt", TimestampType)
            .add("web_qty", LongType)
            .add("fst_atm_dt", TimestampType)
            .add("lst_atm_dt", TimestampType)
            .add("atm_qty", LongType)
            .add("fst_arm_dt", TimestampType)
            .add("lst_arm_dt", TimestampType)
            .add("arm_qty", LongType)
            .add("fst_sberid_dt", TimestampType)
            .add("lst_sberid_dt", TimestampType)
            .add("sberid_qty", LongType)
            .add("tot_day_qty", LongType)
            .add("mobil_day_qty", LongType)
            .add("web_day_qty", LongType)
            .add("atm_day_qty", LongType)
            .add("arm_day_qty", LongType)
            .add("sberid_day_qty", LongType);
    private String lastLoadStart;
    private String firstDayOfMonth;

    @Override
    public void addDatafixes() {
        dataFixApi.castColumnIfNotCasted("epk_id", LongType);
    }

    @Override
    public void init(DatamartServiceFactory datamartServiceFactory) {
        this.firstDayOfMonth = buildDate().withDayOfMonth(1).toString();
        if (!isFirstLoading) {
            this.lastLoadStart = datamartServiceFactory.parametersService().getLastStatistic(LAST_LOAD_START, statistic -> statistic.value);
        }
    }

    @Override
    public Dataset<Row> buildDatamart() {
        addStatistic(LAST_LOAD_START, firstDayOfMonth);

        Column filter = col("epk_id").isNotNull().and(col(DAY_PART).lt(firstDayOfMonth));

        if (!isFirstLoading) {
            filter = filter.and(col(DAY_PART).geq(lastLoadStart));
        }

        final Dataset<Row> filteredSbolLogonAggrDt = targetTable("sbol_logon_aggr_dt")
                .where(filter)
                .select(
                        col("epk_id"),
                        col("login_id"),
                        col("device_info"),
                        col("application"),
                        when(col("application").equalTo(PHIZ_SBER_ID), "INTERNET_BANK_SBERID").otherwise(col("channel_type"))
                                .as("channel_type"),
                        col("tb_nmb"),
                        col("osb_nmb"),
                        col("vsp_nmb"),
                        col("schema"),
                        col("logon_dt"),
                        col("cnt_logon"),
                        col(DAY_PART)
                ).withColumn("channel_type_all",
                        coalesce(
                                col("channel_type"),
                                when(col("application").like(MOBIL), "MAPI")
                                        .when(col("application").equalTo(PHIZ_IC), "WEB")
                                        .when(col("application").like(ATM2), "ATM")
                                        .when(col("application").equalTo(PHIZ_IA), "SERVICE")
                                        .otherwise(null)
                        )
                );

        final Dataset<Row> save_filtered_sbol_aggr_dt = saveTemp(filteredSbolLogonAggrDt, "filtered_sbol_aggr_dt");

        if (!isFirstLoading && isEmpty(filteredSbolLogonAggrDt)) {
            log.warn("Filtered dataframe is empty. Default statistics disabled. Build is stopped.");
            disableDefaultStatistics();
            return DataFrameUtils.createDF(dc.context(), SBOL_LOGON_AGGR_SCHEMA);
        }

        final Dataset<Row> allAggrMonth = filteredSbolLogonAggrDt
                .withColumn("epk_id", coalesce(filteredSbolLogonAggrDt.col("epk_id").cast(LongType), lit(-1L)))
                .groupBy(
                        col("epk_id"),
                        substring(col(DAY_PART), 1, 7).as("mnth")
                )
                .agg(
                        min(col("logon_dt")).as("fst_logon_dt"),
                        max(col("logon_dt")).as("lst_logon_dt"),
                        sum(col("cnt_logon")).as("tot_qty"),
                        min(when(col("channel_type_all").isin(MB), col("logon_dt")).otherwise(null)).as("fst_mobil_dt"),
                        max(when(col("channel_type_all").isin(MB), col("logon_dt")).otherwise(null)).as("lst_mobil_dt"),
                        sum(when(col("channel_type_all").isin(MB), col("cnt_logon")).otherwise(0)).as("mobil_qty"),
                        min(when(col("channel_type_all").isin(WEB), col("logon_dt")).otherwise(null)).as("fst_web_dt"),
                        max(when(col("channel_type_all").isin(WEB), col("logon_dt")).otherwise(null)).as("lst_web_dt"),
                        sum(when(col("channel_type_all").isin(WEB), col("cnt_logon")).otherwise(0)).as("web_qty"),
                        min(when(col("channel_type_all").isin(ATM), col("logon_dt")).otherwise(null)).as("fst_atm_dt"),
                        max(when(col("channel_type_all").isin(ATM), col("logon_dt")).otherwise(null)).as("lst_atm_dt"),
                        sum(when(col("channel_type_all").isin(ATM), col("cnt_logon")).otherwise(0)).as("atm_qty"),
                        min(when(col("channel_type_all").isin(SERVICE), col("logon_dt")).otherwise(null)).as("fst_arm_dt"),
                        max(when(col("channel_type_all").isin(SERVICE), col("logon_dt")).otherwise(null)).as("lst_arm_dt"),
                        sum(when(col("channel_type_all").isin(SERVICE), col("cnt_logon")).otherwise(0)).as("arm_qty"),
                        min(when(col("channel_type_all").isin(SBER_ID), col("logon_dt")).otherwise(null)).as("fst_sberid_dt"),
                        max(when(col("channel_type_all").isin(SBER_ID), col("logon_dt")).otherwise(null)).as("lst_sberid_dt"),
                        sum(when(col("channel_type_all").isin(SBER_ID), col("cnt_logon")).otherwise(0)).as("sberid_qty"),
                        count("*").as("tot_day_qty"),
                        sum(when(col("channel_type_all").isin(MB), 1).otherwise(0)).as("mobil_day_qty"),
                        sum(when(col("channel_type_all").isin(WEB), 1).otherwise(0)).as("web_day_qty"),
                        sum(when(col("channel_type_all").isin(ATM), 1).otherwise(0)).as("atm_day_qty"),
                        sum(when(col("channel_type_all").isin(SERVICE), 1).otherwise(0)).as("arm_day_qty"),
                        sum(when(col("channel_type_all").isin(SBER_ID), 1).otherwise(0)).as("sberid_day_qty")
                );

        Dataset<Row> result;

        if (isFirstLoading) {
            final Dataset<Row> minLogonMonth = allAggrMonth
                    .groupBy(col("epk_id"))
                    .agg(min(col("fst_logon_dt")).as("fst_dt"));

            final Dataset<Row> generatedMonths = generateMonthsDataset(dc.context(), LocalDate.of(2015, 1, 1), buildDate().minusMonths(1));
            final Dataset<Row> monthsByClients = minLogonMonth
                    .join(generatedMonths, generatedMonths.col("month").geq(substring(minLogonMonth.col("fst_dt"), 1, 7)), "left")
                    .select(
                            minLogonMonth.col("epk_id"),
                            generatedMonths.col("month").as("mnth")
                    );

            result = monthsByClients
                    .join(allAggrMonth, allAggrMonth.col("mnth").equalTo(monthsByClients.col("mnth"))
                            .and(allAggrMonth.col("epk_id").equalTo(monthsByClients.col("epk_id"))), "left")
                    .select(
                            monthsByClients.col("epk_id"),
                            monthsByClients.col("mnth").as(MONTH_PART),
                            allAggrMonth.col("fst_logon_dt"),
                            allAggrMonth.col("lst_logon_dt"),
                            allAggrMonth.col("tot_qty"),
                            allAggrMonth.col("fst_mobil_dt"),
                            allAggrMonth.col("lst_mobil_dt"),
                            allAggrMonth.col("mobil_qty"),
                            allAggrMonth.col("fst_web_dt"),
                            allAggrMonth.col("lst_web_dt"),
                            allAggrMonth.col("web_qty"),
                            allAggrMonth.col("fst_atm_dt"),
                            allAggrMonth.col("lst_atm_dt"),
                            allAggrMonth.col("atm_qty"),
                            allAggrMonth.col("fst_arm_dt"),
                            allAggrMonth.col("lst_arm_dt"),
                            allAggrMonth.col("arm_qty"),
                            allAggrMonth.col("fst_sberid_dt"),
                            allAggrMonth.col("lst_sberid_dt"),
                            allAggrMonth.col("sberid_qty"),
                            allAggrMonth.col("tot_day_qty"),
                            allAggrMonth.col("mobil_day_qty"),
                            allAggrMonth.col("web_day_qty"),
                            allAggrMonth.col("atm_day_qty"),
                            allAggrMonth.col("arm_day_qty"),
                            allAggrMonth.col("sberid_day_qty")
                    );
        } else {
            final Dataset<Row> logonDm = self()
                    .where(col(MONTH_PART).equalTo(substring(add_months(lit(lastLoadStart), -1), 1, 7)))
                    .select(
                            col("epk_id"),
                            col(MONTH_PART)
                    );

            result = allAggrMonth.join(logonDm, logonDm.col("epk_id").equalTo(allAggrMonth.col("epk_id")), "full")
                    .select(
                            coalesce(allAggrMonth.col("epk_id"), logonDm.col("epk_id")).as("epk_id"),
                            coalesce(allAggrMonth.col("mnth"), lit(lastLoadStart.substring(0, 7))).as(MONTH_PART),
                            allAggrMonth.col("fst_logon_dt"),
                            allAggrMonth.col("lst_logon_dt"),
                            allAggrMonth.col("tot_qty"),
                            allAggrMonth.col("fst_mobil_dt"),
                            allAggrMonth.col("lst_mobil_dt"),
                            allAggrMonth.col("mobil_qty"),
                            allAggrMonth.col("fst_web_dt"),
                            allAggrMonth.col("lst_web_dt"),
                            allAggrMonth.col("web_qty"),
                            allAggrMonth.col("fst_atm_dt"),
                            allAggrMonth.col("lst_atm_dt"),
                            allAggrMonth.col("atm_qty"),
                            allAggrMonth.col("fst_arm_dt"),
                            allAggrMonth.col("lst_arm_dt"),
                            allAggrMonth.col("arm_qty"),
                            allAggrMonth.col("fst_sberid_dt"),
                            allAggrMonth.col("lst_sberid_dt"),
                            allAggrMonth.col("sberid_qty"),
                            allAggrMonth.col("tot_day_qty"),
                            allAggrMonth.col("mobil_day_qty"),
                            allAggrMonth.col("web_day_qty"),
                            allAggrMonth.col("atm_day_qty"),
                            allAggrMonth.col("arm_day_qty"),
                            allAggrMonth.col("sberid_day_qty")
                    );
        }
        return result;
    }

    public static void main(String[] args) {
        runner().run(SbolLogonAggregate.class);
    }
}
