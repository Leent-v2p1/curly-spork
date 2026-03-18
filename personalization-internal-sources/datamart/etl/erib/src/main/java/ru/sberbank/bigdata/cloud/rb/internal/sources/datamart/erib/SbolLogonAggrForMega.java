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
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.DataFrameUtils;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.*;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.AutoConfigDatamartRunner.runner;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.FieldConstants.MONTH_PART;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId.LAST_LOAD_START;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.SparkSQLUtil.isEmpty;

@DatamartRef(id = "custom_rb_sbol.sbol_logon_aggr_for_mega", name = "Агрегаты по логинам СБОЛ для Мегавитрины")
@PartialReplace(partitioning = MONTH_PART)
public class SbolLogonAggrForMega extends Datamart {

    public static final StructType SBOL_LOGON_AGGR_FOR_MEGA_SCHEMA = new StructType()
            .add("epk_id", LongType)
            .add("month_part", StringType)
            .add("sbol_fst_login_ever_dt", StringType)
            .add("sbol_atm_fst_login_ever_dt", StringType)
            .add("sbol_mob_fst_login_ever_dt", StringType)
            .add("sbol_web_fst_login_ever_dt", StringType)
            .add("sbol_lst_login_ever_dt", StringType)
            .add("sbol_atm_lst_login_ever_dt", StringType)
            .add("sbol_mob_lst_login_ever_dt", StringType)
            .add("sbol_web_lst_login_ever_dt", StringType)
            .add("sbol_fst_login_dt", TimestampType)
            .add("sbol_atm_fst_login_dt", TimestampType)
            .add("sbol_mob_fst_login_dt", TimestampType)
            .add("sbol_web_fst_login_dt", TimestampType)
            .add("sbol_lst_login_dt", TimestampType)
            .add("sbol_atm_lst_login_dt", TimestampType)
            .add("sbol_mob_lst_login_dt", TimestampType)
            .add("sbol_web_lst_login_dt", TimestampType)
            .add("sbol_1m_login_flag", IntegerType)
            .add("sbol_atm_1m_login_flag", IntegerType)
            .add("sbol_mob_1m_login_flag", IntegerType)
            .add("sbol_web_1m_login_flag", IntegerType)
            .add("sbol_3m_login_flag", IntegerType)
            .add("sbol_atm_3m_login_flag", IntegerType)
            .add("sbol_mob_3m_login_flag", IntegerType)
            .add("sbol_web_3m_login_flag", IntegerType)
            .add("sbol_mnth_fst_dt_qty", DoubleType)
            .add("sbol_mnth_lst_dt_qty", DoubleType)
            .add("sbol_mnth_atm_lst_dt_qty", DoubleType)
            .add("sbol_mnth_mob_lst_dt_qty", DoubleType)
            .add("sbol_mnth_web_lst_dt_qty", DoubleType);
    private static final String MAX_DATE = "2099-01-01";
    private static final String MIN_DATE = "1900-01-01";
    private final Logger log = LoggerFactory.getLogger(SbolLogonAggrForMega.class);
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
        addStatistic(LAST_LOAD_START, buildDate().toString());

        final Dataset<Row> sbolLogonAggr = targetTable("sbol_logon_aggr");
        final String currentMonth = buildDate().toString().substring(0, 7);

        Column filter = col(MONTH_PART).lt(currentMonth);

        if (!isFirstLoading) {
            filter = filter.and(col(MONTH_PART).geq((substring(add_months(lit(lastLoadStart), -2), 1, 7))));
        }

        final WindowSpec unboundedPrecedingWindow = Window.partitionBy(col("epk_id"))
                .orderBy(col(MONTH_PART))
                .rowsBetween(Window.unboundedPreceding(), Window.currentRow());

        final WindowSpec twoPrecedingWindow = Window.partitionBy(col("epk_id"))
                .orderBy(col(MONTH_PART))
                .rowsBetween(Window.currentRow() - 2, Window.currentRow());

        final Dataset<Row> filteredSbolLogonAggr = sbolLogonAggr
                .where(filter)
                .select(
                        coalesce(col("epk_id").cast(LongType), lit(-1L)).as("epk_id"),
                        col(MONTH_PART),
                        col("fst_logon_dt").as("sbol_fst_login_dt"),
                        col("fst_atm_dt").as("sbol_atm_fst_login_dt"),
                        col("fst_mobil_dt").as("sbol_mob_fst_login_dt"),
                        col("fst_web_dt").as("sbol_web_fst_login_dt"),
                        col("lst_logon_dt").as("sbol_lst_login_dt"),
                        col("lst_atm_dt").as("sbol_atm_lst_login_dt"),
                        col("lst_mobil_dt").as("sbol_mob_lst_login_dt"),
                        col("lst_web_dt").as("sbol_web_lst_login_dt"),
                        when(col("tot_qty").gt(0), 1).otherwise(0).as("sbol_1m_login_flag"),
                        when(col("atm_qty").gt(0), 1).otherwise(0).as("sbol_atm_1m_login_flag"),
                        when(col("mobil_qty").gt(0), 1).otherwise(0).as("sbol_mob_1m_login_flag"),
                        when(col("web_qty").gt(0), 1).otherwise(0).as("sbol_web_1m_login_flag"),
                        when(sum("tot_qty").over(twoPrecedingWindow).gt(0), 1).otherwise(0).as("sbol_3m_login_flag"),
                        when(sum("atm_qty").over(twoPrecedingWindow).gt(0), 1).otherwise(0).as("sbol_atm_3m_login_flag"),
                        when(sum("mobil_qty").over(twoPrecedingWindow).gt(0), 1).otherwise(0).as("sbol_mob_3m_login_flag"),
                        when(sum("web_qty").over(twoPrecedingWindow).gt(0), 1).otherwise(0).as("sbol_web_3m_login_flag")
                );

        Dataset<Row> result;

        if (isFirstLoading) {

            final Dataset<Row> logonAggrByClient = filteredSbolLogonAggr
                    .select(
                            col("*"),
                            min("sbol_fst_login_dt").over(unboundedPrecedingWindow).cast(StringType).as("sbol_fst_login_ever_dt"),
                            min("sbol_atm_fst_login_dt").over(unboundedPrecedingWindow).cast(StringType).as("sbol_atm_fst_login_ever_dt"),
                            min("sbol_mob_fst_login_dt").over(unboundedPrecedingWindow).cast(StringType).as("sbol_mob_fst_login_ever_dt"),
                            min("sbol_web_fst_login_dt").over(unboundedPrecedingWindow).cast(StringType).as("sbol_web_fst_login_ever_dt"),
                            max("sbol_lst_login_dt").over(unboundedPrecedingWindow).cast(StringType).as("sbol_lst_login_ever_dt"),
                            max("sbol_atm_lst_login_dt").over(unboundedPrecedingWindow).cast(StringType).as("sbol_atm_lst_login_ever_dt"),
                            max("sbol_mob_lst_login_dt").over(unboundedPrecedingWindow).cast(StringType).as("sbol_mob_lst_login_ever_dt"),
                            max("sbol_web_lst_login_dt").over(unboundedPrecedingWindow).cast(StringType).as("sbol_web_lst_login_ever_dt")
                    );

            result = logonAggrByClient
                    .select(
                            col("*"),
                            monthsCount("sbol_fst_login_ever_dt").as("sbol_mnth_fst_dt_qty"),
                            monthsCount("sbol_lst_login_ever_dt").as("sbol_mnth_lst_dt_qty"),
                            monthsCount("sbol_atm_lst_login_ever_dt").as("sbol_mnth_atm_lst_dt_qty"),
                            monthsCount("sbol_mob_lst_login_ever_dt").as("sbol_mnth_mob_lst_dt_qty"),
                            monthsCount("sbol_web_lst_login_ever_dt").as("sbol_mnth_web_lst_dt_qty")
                    );
        } else {

            final Dataset<Row> sbolLogonAggrForCount = saveTemp(sbolLogonAggr
                    .where(col(MONTH_PART).lt(currentMonth).and(col(MONTH_PART).geq(substring(lit(lastLoadStart), 1, 7)))), "sbolLogonAggrForCount");

            if (isEmpty(sbolLogonAggrForCount)) {
                log.warn("Filtered dataframe is empty. Default statistics disabled. Build is stopped.");
                disableDefaultStatistics();
                return DataFrameUtils.createDF(dc.context(), SBOL_LOGON_AGGR_FOR_MEGA_SCHEMA);
            }

            final Dataset<Row> oldLogonMega = self()
                    .where(col(MONTH_PART).equalTo(substring(add_months(lit(lastLoadStart), -1), 1, 7)))
                    .select(
                            coalesce(col("epk_id").cast(LongType), lit(-1L)).as("epk_id"),
                            col(MONTH_PART),
                            col("sbol_fst_login_ever_dt"),
                            col("sbol_atm_fst_login_ever_dt"),
                            col("sbol_mob_fst_login_ever_dt"),
                            col("sbol_web_fst_login_ever_dt"),
                            col("sbol_lst_login_ever_dt"),
                            col("sbol_atm_lst_login_ever_dt"),
                            col("sbol_mob_lst_login_ever_dt"),
                            col("sbol_web_lst_login_ever_dt")
                    );

            final Dataset<Row> newLogonMega = filteredSbolLogonAggr
                    .where(filteredSbolLogonAggr.col(MONTH_PART).geq(substring(lit(lastLoadStart), 1, 7)))
                    .select(col("*"));

            final Dataset<Row> updateEverData = oldLogonMega
                    .join(newLogonMega, newLogonMega.col("epk_id").equalTo(oldLogonMega.col("epk_id")), "full")
                    .select(
                            coalesce(newLogonMega.col("epk_id"), oldLogonMega.col("epk_id")).as("epk_id"),
                            coalesce(newLogonMega.col(MONTH_PART), lit(lastLoadStart.substring(0, 7))).as(MONTH_PART),
                            newLogonMega.col("sbol_fst_login_dt").as("sbol_fst_login_dt"),
                            newLogonMega.col("sbol_atm_fst_login_dt").as("sbol_atm_fst_login_dt"),
                            newLogonMega.col("sbol_mob_fst_login_dt").as("sbol_mob_fst_login_dt"),
                            newLogonMega.col("sbol_web_fst_login_dt").as("sbol_web_fst_login_dt"),
                            newLogonMega.col("sbol_lst_login_dt").as("sbol_lst_login_dt"),
                            newLogonMega.col("sbol_atm_lst_login_dt").as("sbol_atm_lst_login_dt"),
                            newLogonMega.col("sbol_mob_lst_login_dt").as("sbol_mob_lst_login_dt"),
                            newLogonMega.col("sbol_web_lst_login_dt").as("sbol_web_lst_login_dt"),
                            newLogonMega.col("sbol_1m_login_flag").as("sbol_1m_login_flag"),
                            newLogonMega.col("sbol_atm_1m_login_flag").as("sbol_atm_1m_login_flag"),
                            newLogonMega.col("sbol_mob_1m_login_flag").as("sbol_mob_1m_login_flag"),
                            newLogonMega.col("sbol_web_1m_login_flag").as("sbol_web_1m_login_flag"),
                            newLogonMega.col("sbol_3m_login_flag").as("sbol_3m_login_flag"),
                            newLogonMega.col("sbol_atm_3m_login_flag").as("sbol_atm_3m_login_flag"),
                            newLogonMega.col("sbol_mob_3m_login_flag").as("sbol_mob_3m_login_flag"),
                            newLogonMega.col("sbol_web_3m_login_flag").as("sbol_web_3m_login_flag"),

                            when(coalesce(oldLogonMega.col("sbol_fst_login_ever_dt"), lit(MAX_DATE))
                                            .gt(coalesce(newLogonMega.col("sbol_fst_login_dt"), lit(MAX_DATE))),
                                    coalesce(newLogonMega.col("sbol_fst_login_dt"), lit(MAX_DATE)))
                                    .otherwise(coalesce(oldLogonMega.col("sbol_fst_login_ever_dt"), lit(MAX_DATE)))
                                    .as("sbol_fst_login_ever_dt"),

                            when(coalesce(oldLogonMega.col("sbol_atm_fst_login_ever_dt"), lit(MAX_DATE))
                                            .gt(coalesce(newLogonMega.col("sbol_atm_fst_login_dt"), lit(MAX_DATE))),
                                    coalesce(newLogonMega.col("sbol_atm_fst_login_dt"), lit(MAX_DATE)))
                                    .otherwise(coalesce(oldLogonMega.col("sbol_atm_fst_login_ever_dt"), lit(MAX_DATE)))
                                    .as("sbol_atm_fst_login_ever_dt"),
                            when(coalesce(oldLogonMega.col("sbol_mob_fst_login_ever_dt"), lit(MAX_DATE))
                                            .gt(coalesce(newLogonMega.col("sbol_mob_fst_login_dt"), lit(MAX_DATE))),
                                    coalesce(newLogonMega.col("sbol_mob_fst_login_dt"), lit(MAX_DATE)))
                                    .otherwise(coalesce(oldLogonMega.col("sbol_mob_fst_login_ever_dt"), lit(MAX_DATE)))
                                    .as("sbol_mob_fst_login_ever_dt"),
                            when(coalesce(oldLogonMega.col("sbol_web_fst_login_ever_dt"), lit(MAX_DATE))
                                            .gt(coalesce(newLogonMega.col("sbol_web_fst_login_dt"), lit(MAX_DATE))),
                                    coalesce(newLogonMega.col("sbol_web_fst_login_dt"), lit(MAX_DATE)))
                                    .otherwise(coalesce(oldLogonMega.col("sbol_web_fst_login_ever_dt"), lit(MAX_DATE)))
                                    .as("sbol_web_fst_login_ever_dt"),
                            when(coalesce(oldLogonMega.col("sbol_lst_login_ever_dt"), lit(MIN_DATE))
                                            .gt(coalesce(newLogonMega.col("sbol_lst_login_dt"), lit(MIN_DATE))),
                                    coalesce(oldLogonMega.col("sbol_lst_login_ever_dt"), lit(MIN_DATE)))
                                    .otherwise(coalesce(newLogonMega.col("sbol_lst_login_dt"), lit(MIN_DATE)))
                                    .as("sbol_lst_login_ever_dt"),
                            when(coalesce(oldLogonMega.col("sbol_atm_lst_login_ever_dt"), lit(MIN_DATE))
                                            .gt(coalesce(newLogonMega.col("sbol_atm_lst_login_dt"), lit(MIN_DATE))),
                                    coalesce(oldLogonMega.col("sbol_atm_lst_login_ever_dt"), lit(MIN_DATE)))
                                    .otherwise(coalesce(newLogonMega.col("sbol_atm_lst_login_dt"), lit(MIN_DATE)))
                                    .as("sbol_atm_lst_login_ever_dt"),
                            when(coalesce(oldLogonMega.col("sbol_mob_lst_login_ever_dt"), lit(MIN_DATE))
                                            .gt(coalesce(newLogonMega.col("sbol_mob_lst_login_dt"), lit(MIN_DATE))),
                                    coalesce(oldLogonMega.col("sbol_mob_lst_login_ever_dt"), lit(MIN_DATE)))
                                    .otherwise(coalesce(newLogonMega.col("sbol_mob_lst_login_dt"), lit(MIN_DATE)))
                                    .as("sbol_mob_lst_login_ever_dt"),
                            when(coalesce(oldLogonMega.col("sbol_web_lst_login_ever_dt"), lit(MIN_DATE))
                                            .gt(coalesce(newLogonMega.col("sbol_web_lst_login_dt"), lit(MIN_DATE))),
                                    coalesce(oldLogonMega.col("sbol_web_lst_login_ever_dt"), lit(MIN_DATE)))
                                    .otherwise(coalesce(newLogonMega.col("sbol_web_lst_login_dt"), lit(MIN_DATE)))
                                    .as("sbol_web_lst_login_ever_dt")
                    );
            result = updateEverData
                    .select(
                            col("epk_id"),
                            col(MONTH_PART),
                            col("sbol_fst_login_dt"),
                            col("sbol_atm_fst_login_dt"),
                            col("sbol_mob_fst_login_dt"),
                            col("sbol_web_fst_login_dt"),
                            col("sbol_lst_login_dt"),
                            col("sbol_atm_lst_login_dt"),
                            col("sbol_mob_lst_login_dt"),
                            col("sbol_web_lst_login_dt"),
                            col("sbol_1m_login_flag"),
                            col("sbol_atm_1m_login_flag"),
                            col("sbol_mob_1m_login_flag"),
                            col("sbol_web_1m_login_flag"),
                            col("sbol_3m_login_flag"),
                            col("sbol_atm_3m_login_flag"),
                            col("sbol_mob_3m_login_flag"),
                            col("sbol_web_3m_login_flag"),
                            when(col("sbol_fst_login_ever_dt").equalTo(MAX_DATE), null)
                                    .otherwise(col("sbol_fst_login_ever_dt")).as("sbol_fst_login_ever_dt"),
                            when(col("sbol_atm_fst_login_ever_dt").equalTo(MAX_DATE), null)
                                    .otherwise(col("sbol_atm_fst_login_ever_dt")).as("sbol_atm_fst_login_ever_dt"),
                            when(col("sbol_mob_fst_login_ever_dt").equalTo(MAX_DATE), null)
                                    .otherwise(col("sbol_mob_fst_login_ever_dt")).as("sbol_mob_fst_login_ever_dt"),
                            when(col("sbol_web_fst_login_ever_dt").equalTo(MAX_DATE), null)
                                    .otherwise(col("sbol_web_fst_login_ever_dt")).as("sbol_web_fst_login_ever_dt"),
                            when(col("sbol_lst_login_ever_dt").equalTo(MIN_DATE), null)
                                    .otherwise(col("sbol_lst_login_ever_dt")).as("sbol_lst_login_ever_dt"),
                            when(col("sbol_atm_lst_login_ever_dt").equalTo(MIN_DATE), null)
                                    .otherwise(col("sbol_atm_lst_login_ever_dt")).as("sbol_atm_lst_login_ever_dt"),
                            when(col("sbol_mob_lst_login_ever_dt").equalTo(MIN_DATE), null)
                                    .otherwise(col("sbol_mob_lst_login_ever_dt")).as("sbol_mob_lst_login_ever_dt"),
                            when(col("sbol_web_lst_login_ever_dt").equalTo(MIN_DATE), null)
                                    .otherwise(col("sbol_web_lst_login_ever_dt")).as("sbol_web_lst_login_ever_dt"),
                            when(col("sbol_fst_login_ever_dt").equalTo(MAX_DATE), null)
                                    .otherwise(monthsCount("sbol_fst_login_ever_dt")).as("sbol_mnth_fst_dt_qty"),
                            when(col("sbol_lst_login_ever_dt").equalTo(MIN_DATE), null)
                                    .otherwise(monthsCount("sbol_lst_login_ever_dt")).as("sbol_mnth_lst_dt_qty"),
                            when(col("sbol_atm_lst_login_ever_dt").equalTo(MIN_DATE), null)
                                    .otherwise(monthsCount("sbol_atm_lst_login_ever_dt")).as("sbol_mnth_atm_lst_dt_qty"),
                            when(col("sbol_mob_lst_login_ever_dt").equalTo(MIN_DATE), null)
                                    .otherwise(monthsCount("sbol_mob_lst_login_ever_dt")).as("sbol_mnth_mob_lst_dt_qty"),
                            when(col("sbol_web_lst_login_ever_dt").equalTo(MIN_DATE), null)
                                    .otherwise(monthsCount("sbol_web_lst_login_ever_dt")).as("sbol_mnth_web_lst_dt_qty")
                    );
        }

        return result;
    }

    private Column monthsCount(String colName) {
        return months_between(date_add(add_months(concat(col(MONTH_PART), lit("-01")), 1), -1), col(colName));
    }

    public static void main(String[] args) {
        runner().run(SbolLogonAggrForMega.class);
    }
}
