package ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.erib;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.DatamartRef;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.FullReplace;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.DatamartServiceFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.Datamart;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.DataFrameUtils;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.SparkSQLUtil;

import java.sql.Timestamp;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.TimestampType;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.AutoConfigDatamartRunner.runner;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.SchemaNames.CARD;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.SchemaNames.SBOL;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.DecimalTypes.DECIMAL_38_0;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.FieldConstants.DAY_PART;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId.LAST_LOAD_START;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.SparkSQLUtil.isEmpty;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.erib.SbolConstants.*;

@DatamartRef(id = "custom_rb_sbol.sbol_logon_hist", name = "История по логинам СБОЛ")
@FullReplace
public class SbolLogonHist extends Datamart {

    private static final Logger log = LoggerFactory.getLogger(SbolLogonHist.class);
    public static final StructType SBOL_LOGON_HIST_SCHEMA = new StructType()
            .add("epk_id", LongType)
            .add("fst_logon_ever_dt", TimestampType)
            .add("lst_logon_ever_dt", TimestampType)
            .add("fst_mobil_ever_dt", TimestampType)
            .add("lst_mobil_ever_dt", TimestampType)
            .add("fst_web_ever_dt", TimestampType)
            .add("lst_web_ever_dt", TimestampType)
            .add("fst_atm_ever_dt", TimestampType)
            .add("lst_atm_ever_dt", TimestampType)
            .add("fst_arm_ever_dt", TimestampType)
            .add("lst_arm_ever_dt", TimestampType)
            .add("fst_sberid_ever_dt", TimestampType)
            .add("lst_sberid_ever_dt", TimestampType)
            .add("login_id", DECIMAL_38_0)
            .add("device_info", StringType)
            .add("application", StringType)
            .add("channel_type", StringType)
            .add("tb_nmb", StringType)
            .add("osb_nmb", StringType)
            .add("vsp_nmb", StringType)
            .add("schema", StringType);
    private static final Timestamp MAX_DATE = Timestamp.valueOf("2099-01-01 00:00:00.0");
    private static final Timestamp MIN_DATE = Timestamp.valueOf("1900-01-01 00:00:00.0");
    private Timestamp lastLoadStart;

    @Override
    public void addDatafixes() {
        dataFixApi.castColumnIfNotCasted("epk_id", LongType);
        dataFixApi.addColumnIfMissing("channel_type", StringType);
    }

    @Override
    public void init(DatamartServiceFactory dsf) {
        if (!isFirstLoading) {
            this.lastLoadStart = dsf.parametersService().getLastTimestampStatValue(LAST_LOAD_START);
        }
    }

    @Override
    public Dataset<Row> buildDatamart() {
        final Dataset<Row> sbolLogonAggrDt = targetTable("sbol_logon_aggr_dt");

        Column filter = col("epk_id").isNotNull().and(col("epk_id").notEqual(lit(-1)))
                .and(col(DAY_PART).lt(startDay()));
        if (!isFirstLoading) {
            filter = filter.and(col(DAY_PART).geq(lastLoadStart));
        }

        final Dataset<Row> filteredSbolAggrDt = sbolLogonAggrDt
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
                        col("logon_dt")
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

        final Dataset<Row> save_filtered_sbol_aggr_dt = saveTemp(filteredSbolAggrDt, "filtered_sbol_aggr_dt");

        if (!isFirstLoading && isEmpty(filteredSbolAggrDt)) {
            log.warn("Filtered dataframe is empty. Default statistics disabled. Build is stopped.");
            disableDefaultStatistics();
            return DataFrameUtils.createDF(dc.context(), SBOL_LOGON_HIST_SCHEMA);
        }

        final Dataset<Row> allAggrMonth = filteredSbolAggrDt
                .withColumn("epk_id", coalesce(filteredSbolAggrDt.col("epk_id").cast(LongType), lit(-1L)))
                .groupBy(col("epk_id"))
                .agg(
                        min(col("logon_dt")).as("fst_logon_ever_dt"),
                        max(col("logon_dt")).as("lst_logon_ever_dt"),

                        min(when(col("channel_type_all").isin(MB), col("logon_dt")).otherwise(null)).as("fst_mobil_ever_dt"),
                        max(when(col("channel_type_all").isin(MB), col("logon_dt")).otherwise(null)).as("lst_mobil_ever_dt"),

                        min(when(col("channel_type_all").isin(WEB), col("logon_dt")).otherwise(null)).as("fst_web_ever_dt"),
                        max(when(col("channel_type_all").isin(WEB), col("logon_dt")).otherwise(null)).as("lst_web_ever_dt"),

                        min(when(col("channel_type_all").isin(ATM), col("logon_dt")).otherwise(null)).as("fst_atm_ever_dt"),
                        max(when(col("channel_type_all").isin(ATM), col("logon_dt")).otherwise(null)).as("lst_atm_ever_dt"),

                        min(when(col("channel_type_all").isin(SERVICE), col("logon_dt")).otherwise(null)).as("fst_arm_ever_dt"),
                        max(when(col("channel_type_all").isin(SERVICE), col("logon_dt")).otherwise(null)).as("lst_arm_ever_dt"),

                        min(when(col("channel_type_all").isin(SBER_ID), col("logon_dt")).otherwise(null)).as("fst_sberid_ever_dt"),
                        max(when(col("channel_type_all").isin(SBER_ID), col("logon_dt")).otherwise(null)).as("lst_sberid_ever_dt")
                ).select(
                        col("epk_id"),
                        col("fst_logon_ever_dt"),
                        col("lst_logon_ever_dt"),
                        col("fst_mobil_ever_dt"),
                        col("lst_mobil_ever_dt"),
                        col("fst_web_ever_dt"),
                        col("lst_web_ever_dt"),
                        col("fst_atm_ever_dt"),
                        col("lst_atm_ever_dt"),
                        col("fst_arm_ever_dt"),
                        col("lst_arm_ever_dt"),
                        col("fst_sberid_ever_dt"),
                        col("lst_sberid_ever_dt")
                )
                ;

        final Dataset<Row> lastLogInf = filteredSbolAggrDt
                .select(
                        col("epk_id"),
                        col("login_id"),
                        col("device_info"),
                        col("application"),
                        col("channel_type"),
                        col("tb_nmb"),
                        col("osb_nmb"),
                        col("vsp_nmb"),
                        col("schema"),
                        row_number().over(Window.partitionBy(col("epk_id")).orderBy(col("logon_dt").desc())).as("rn"))
                .where(col("rn").equalTo(lit(1)))
                ;


        final Dataset<Row> unionOfAggrAndLastLog = allAggrMonth
                .join(lastLogInf, allAggrMonth.col("epk_id").equalTo(lastLogInf.col("epk_id")), "left")
                .select(
                        allAggrMonth.col("epk_id"),
                        allAggrMonth.col("fst_logon_ever_dt"),
                        allAggrMonth.col("lst_logon_ever_dt"),
                        allAggrMonth.col("fst_mobil_ever_dt"),
                        allAggrMonth.col("lst_mobil_ever_dt"),
                        allAggrMonth.col("fst_web_ever_dt"),
                        allAggrMonth.col("lst_web_ever_dt"),
                        allAggrMonth.col("fst_atm_ever_dt"),
                        allAggrMonth.col("lst_atm_ever_dt"),
                        allAggrMonth.col("fst_arm_ever_dt"),
                        allAggrMonth.col("lst_arm_ever_dt"),
                        allAggrMonth.col("fst_sberid_ever_dt"),
                        allAggrMonth.col("lst_sberid_ever_dt"),
                        lastLogInf.col("login_id"),
                        lastLogInf.col("device_info"),
                        lastLogInf.col("application"),
                        lastLogInf.col("channel_type"),
                        lastLogInf.col("tb_nmb"),
                        lastLogInf.col("osb_nmb"),
                        lastLogInf.col("vsp_nmb"),
                        lastLogInf.col("schema")
                )
                ;


        final Dataset<Row> result;
        if (isFirstLoading) {
            result = unionOfAggrAndLastLog;
        } else {
            final Dataset<Row> self = sourceTable(SBOL.resolve("SBOL_LOGON_HIST"));
            final Dataset<Row> updatedAggregate = unionOfAggrAndLastLog
                    .join(self, self.col("epk_id").equalTo(unionOfAggrAndLastLog.col("epk_id")), "full")
                    .select(
                            coalesce(coalesce(unionOfAggrAndLastLog.col("epk_id"), self.col("epk_id")).cast(LongType), lit(-1L)).as("epk_id"),
                            coalesce(unionOfAggrAndLastLog.col("login_id"), self.col("login_id")).as("login_id"),
                            coalesce(unionOfAggrAndLastLog.col("device_info"), self.col("device_info")).as("device_info"),
                            coalesce(unionOfAggrAndLastLog.col("application"), self.col("application")).as("application"),
                            coalesce(unionOfAggrAndLastLog.col("channel_type"), self.col("channel_type")).as("channel_type"),
                            coalesce(unionOfAggrAndLastLog.col("tb_nmb"), self.col("tb_nmb")).as("tb_nmb"),
                            coalesce(unionOfAggrAndLastLog.col("osb_nmb"), self.col("osb_nmb")).as("osb_nmb"),
                            coalesce(unionOfAggrAndLastLog.col("vsp_nmb"), self.col("vsp_nmb")).as("vsp_nmb"),
                            coalesce(unionOfAggrAndLastLog.col("schema"), self.col("schema")).as("schema"),
                            when(coalesce(self.col("fst_logon_ever_dt"), lit(MAX_DATE)).gt(coalesce(unionOfAggrAndLastLog.col("fst_logon_ever_dt"), lit(MAX_DATE))),
                                    coalesce(unionOfAggrAndLastLog.col("fst_logon_ever_dt"), lit(MAX_DATE)))
                                    .otherwise(coalesce(self.col("fst_logon_ever_dt"), lit(MAX_DATE))).as("fst_logon_ever_dt"),
                            when(coalesce(self.col("fst_mobil_ever_dt"), lit(MAX_DATE)).gt(coalesce(unionOfAggrAndLastLog.col("fst_mobil_ever_dt"), lit(MAX_DATE))),
                                    coalesce(unionOfAggrAndLastLog.col("fst_mobil_ever_dt"), lit(MAX_DATE)))
                                    .otherwise(coalesce(self.col("fst_mobil_ever_dt"), lit(MAX_DATE))).as("fst_mobil_ever_dt"),
                            when(coalesce(self.col("fst_web_ever_dt"), lit(MAX_DATE)).gt(coalesce(unionOfAggrAndLastLog.col("fst_web_ever_dt"), lit(MAX_DATE))),
                                    coalesce(unionOfAggrAndLastLog.col("fst_web_ever_dt"), lit(MAX_DATE)))
                                    .otherwise(coalesce(self.col("fst_web_ever_dt"), lit(MAX_DATE))).as("fst_web_ever_dt"),
                            when(coalesce(self.col("fst_atm_ever_dt"), lit(MAX_DATE)).gt(coalesce(unionOfAggrAndLastLog.col("fst_atm_ever_dt"), lit(MAX_DATE))),
                                    coalesce(unionOfAggrAndLastLog.col("fst_atm_ever_dt"), lit(MAX_DATE)))
                                    .otherwise(coalesce(self.col("fst_atm_ever_dt"), lit(MAX_DATE))).as("fst_atm_ever_dt"),
                            when(coalesce(self.col("fst_arm_ever_dt"), lit(MAX_DATE)).gt(coalesce(unionOfAggrAndLastLog.col("fst_arm_ever_dt"), lit(MAX_DATE))),
                                    coalesce(unionOfAggrAndLastLog.col("fst_arm_ever_dt"), lit(MAX_DATE)))
                                    .otherwise(coalesce(self.col("fst_arm_ever_dt"), lit(MAX_DATE))).as("fst_arm_ever_dt"),
                            when(coalesce(self.col("fst_sberid_ever_dt"), lit(MAX_DATE)).gt(coalesce(unionOfAggrAndLastLog.col("fst_sberid_ever_dt"), lit(MAX_DATE))),
                                    coalesce(unionOfAggrAndLastLog.col("fst_sberid_ever_dt"), lit(MAX_DATE)))
                                    .otherwise(coalesce(self.col("fst_sberid_ever_dt"), lit(MAX_DATE))).as("fst_sberid_ever_dt"),

                            when(coalesce(self.col("lst_logon_ever_dt"), lit(MIN_DATE)).gt(coalesce(unionOfAggrAndLastLog.col("lst_logon_ever_dt"), lit(MIN_DATE))),
                                    coalesce(self.col("lst_logon_ever_dt"), lit(MIN_DATE)))
                                    .otherwise(coalesce(unionOfAggrAndLastLog.col("lst_logon_ever_dt"), lit(MIN_DATE)))
                                    .as("lst_logon_ever_dt"),
                            when(coalesce(self.col("lst_mobil_ever_dt"), lit(MIN_DATE)).gt(coalesce(unionOfAggrAndLastLog.col("lst_mobil_ever_dt"), lit(MIN_DATE))),
                                    coalesce(self.col("lst_mobil_ever_dt"), lit(MIN_DATE)))
                                    .otherwise(coalesce(unionOfAggrAndLastLog.col("lst_mobil_ever_dt"), lit(MIN_DATE)))
                                    .as("lst_mobil_ever_dt"),
                            when(coalesce(self.col("lst_web_ever_dt"), lit(MIN_DATE)).gt(coalesce(unionOfAggrAndLastLog.col("lst_web_ever_dt"), lit(MIN_DATE))),
                                    coalesce(self.col("lst_web_ever_dt"), lit(MIN_DATE)))
                                    .otherwise(coalesce(unionOfAggrAndLastLog.col("lst_web_ever_dt"), lit(MIN_DATE))).as("lst_web_ever_dt"),
                            when(coalesce(self.col("lst_atm_ever_dt"), lit(MIN_DATE)).gt(coalesce(unionOfAggrAndLastLog.col("lst_atm_ever_dt"), lit(MIN_DATE))),
                                    coalesce(self.col("lst_atm_ever_dt"), lit(MIN_DATE)))
                                    .otherwise(coalesce(unionOfAggrAndLastLog.col("lst_atm_ever_dt"), lit(MIN_DATE))).as("lst_atm_ever_dt"),
                            when(coalesce(self.col("lst_arm_ever_dt"), lit(MIN_DATE)).gt(coalesce(unionOfAggrAndLastLog.col("lst_arm_ever_dt"), lit(MIN_DATE))),
                                    coalesce(self.col("lst_arm_ever_dt"), lit(MIN_DATE)))
                                    .otherwise(coalesce(unionOfAggrAndLastLog.col("lst_arm_ever_dt"), lit(MIN_DATE))).as("lst_arm_ever_dt"),
                            when(coalesce(self.col("lst_sberid_ever_dt"), lit(MIN_DATE)).gt(coalesce(unionOfAggrAndLastLog.col("lst_sberid_ever_dt"), lit(MIN_DATE))),
                                    coalesce(self.col("lst_sberid_ever_dt"), lit(MIN_DATE)))
                                    .otherwise(coalesce(unionOfAggrAndLastLog.col("lst_sberid_ever_dt"), lit(MIN_DATE)))
                                    .as("lst_sberid_ever_dt")
                    );

            result = updatedAggregate
                    .select(
                            updatedAggregate.col("epk_id"),
                            updatedAggregate.col("login_id"),
                            updatedAggregate.col("device_info"),
                            updatedAggregate.col("application"),
                            updatedAggregate.col("channel_type"),
                            updatedAggregate.col("tb_nmb"),
                            updatedAggregate.col("osb_nmb"),
                            updatedAggregate.col("vsp_nmb"),
                            updatedAggregate.col("schema"),
                            when(updatedAggregate.col("fst_logon_ever_dt").equalTo(lit(MAX_DATE)), null)
                                    .otherwise(updatedAggregate.col("fst_logon_ever_dt")).as("fst_logon_ever_dt"),
                            when(updatedAggregate.col("fst_mobil_ever_dt").equalTo(lit(MAX_DATE)), null)
                                    .otherwise(updatedAggregate.col("fst_mobil_ever_dt")).as("fst_mobil_ever_dt"),
                            when(updatedAggregate.col("fst_web_ever_dt").equalTo(lit(MAX_DATE)), null)
                                    .otherwise(updatedAggregate.col("fst_web_ever_dt")).as("fst_web_ever_dt"),
                            when(updatedAggregate.col("fst_atm_ever_dt").equalTo(lit(MAX_DATE)), null)
                                    .otherwise(updatedAggregate.col("fst_atm_ever_dt")).as("fst_atm_ever_dt"),
                            when(updatedAggregate.col("fst_arm_ever_dt").equalTo(lit(MAX_DATE)), null)
                                    .otherwise(updatedAggregate.col("fst_arm_ever_dt")).as("fst_arm_ever_dt"),
                            when(updatedAggregate.col("fst_sberid_ever_dt").equalTo(lit(MAX_DATE)), null)
                                    .otherwise(updatedAggregate.col("fst_sberid_ever_dt")).as("fst_sberid_ever_dt"),

                            when(updatedAggregate.col("lst_logon_ever_dt").equalTo(lit(MIN_DATE)), null)
                                    .otherwise(updatedAggregate.col("lst_logon_ever_dt")).as("lst_logon_ever_dt"),
                            when(updatedAggregate.col("lst_mobil_ever_dt").equalTo(lit(MIN_DATE)), null)
                                    .otherwise(updatedAggregate.col("lst_mobil_ever_dt")).as("lst_mobil_ever_dt"),
                            when(updatedAggregate.col("lst_web_ever_dt").equalTo(lit(MIN_DATE)), null)
                                    .otherwise(updatedAggregate.col("lst_web_ever_dt")).as("lst_web_ever_dt"),
                            when(updatedAggregate.col("lst_atm_ever_dt").equalTo(lit(MIN_DATE)), null)
                                    .otherwise(updatedAggregate.col("lst_atm_ever_dt")).as("lst_atm_ever_dt"),
                            when(updatedAggregate.col("lst_arm_ever_dt").equalTo(lit(MIN_DATE)), null)
                                    .otherwise(updatedAggregate.col("lst_arm_ever_dt")).as("lst_arm_ever_dt"),
                            when(updatedAggregate.col("lst_sberid_ever_dt").equalTo(lit(MIN_DATE)), null)
                                    .otherwise(updatedAggregate.col("lst_sberid_ever_dt")).as("lst_sberid_ever_dt")
                    )
            ;
        }

        final String maxLogonDt = SparkSQLUtil.getAggColumnValue(sbolLogonAggrDt.where(filter), "logon_dt", functions::max).toString();
        addStatistic(LAST_LOAD_START, maxLogonDt);

        return result;
    }

    public static void main(String[] args) {
        runner().run(SbolLogonHist.class);
    }
}
