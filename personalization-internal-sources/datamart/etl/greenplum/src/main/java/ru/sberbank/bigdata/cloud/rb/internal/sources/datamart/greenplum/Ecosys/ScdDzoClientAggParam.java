package ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.greenplum.Ecosys;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.StructType;

import org.apache.spark.storage.StorageLevel;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.DatamartRef;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.PartialReplace;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.DatamartServiceFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.ParametersService;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.Datamart;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.hive.PartitionInfo;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_remover.UpdatedPartitionRemover;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_strategy.HiveSavingStrategy;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_strategy.PartitionedSavingStrategy;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.SysPropertyTool;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.recount.GetRegularTxnFlag;
import ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.greenplum.DepfinTables;

import java.time.LocalDate;
import java.time.Month;
import java.util.*;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.*;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.AutoConfigDatamartRunner.runner;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId.LAST_LOAD_START;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId.PROCESSED_LOADING_ID;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.SparkSQLUtil.getAggColumnValue;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.SparkSQLUtil.unionAll;

@PartialReplace(partitioning = "row_actual_to_dt", saveRemover = UpdatedPartitionRemover.class)
@DatamartRef(id = "custom_rb_depfin.scd_dzo_client_agg_param", name = "DZO client agg params")
public class ScdDzoClientAggParam extends Datamart {
    private static final LocalDate DEFAULT_START_DT = LocalDate.of(2022, Month.JANUARY, 1);

    private LocalDate startDt;
    private LocalDate endDt;
    private String lastLoadId;
    private String lastLoadDt;
    private Column filterEcosysDzoClientAggDay;
    private Column filterEcosysTxnDet;

    private static final int OKKO_ID = 3;
    private static final int ZVUK_ID = 103;
    private static final int MEGAMARKET_ID = 32;

    public static final Object[] OKKO_USER_EVT_ID = {3, 0, 9, 2, 4, 1};
    public static final Object[] ZVUK_USER_EVT_ID = {0, 7, 8};
    public static final Object[] MEGAMARKET_USER_EVT_ID = {0, 10, 11, 12};

    private static final int ONLINE_CINEMA_PARTNER_TYPE_ID = 2;
    private static final int MUSIC_PARTNER_TYPE_ID = 13;

    public static final Object[] MVS_SEGMENT = {"MVS_EXT", "MVS_KEY"};
    public static final Object[] AGE_SEGMENT = {"YOUTH", "PREADULT", "GROWN_UP", "PRIME_AGE", "SENIOR"};
    public static final Object[] ECOM_PARTNER_TYPE_ID = {22, 4, 5, 6};
    public static final Object[] EGROCERY_PARTNER_TYPE_ID = {4, 23};

    private static final String ACTUAL_TO_DT = "9999-12-31";
    private static final int SAVED_INTERVAL = 330;

    public static final StructType REGULAR_TXN_CLIENT_STRUCT = new StructType()
            .add("epk_id", LongType)
            .add("reg_txn", IntegerType);

    public static void main(String[] args) {
        runner().run(ScdDzoClientAggParam.class);
    }

    @Override
    public HiveSavingStrategy customSavingStrategy(DatamartServiceFactory datamartServiceFactory) {
        return new PartitionedSavingStrategy(PartitionInfo.dynamic().add("row_actual_to_dt").create());
    }

    public void init(DatamartServiceFactory datamartServiceFactory) {
        final ParametersService parametersService = datamartServiceFactory.parametersService();
        if (isFirstLoading || isReload) {
            this.endDt = parametersService.endCtlParameter().orElse(buildDate());
            this.startDt = parametersService.startCtlParameter().orElse(DEFAULT_START_DT);
            this.filterEcosysDzoClientAggDay = col("d2").geq(startDt).and(col("d2").lt(endDt));
            this.filterEcosysTxnDet = col("txn_dt").geq(startDt).and(col("txn_dt").lt(endDt));
        } else {
            this.lastLoadId = parametersService.getLastStatistic(PROCESSED_LOADING_ID, statistic -> statistic.value);
            this.lastLoadDt = parametersService.getLastStatistic(LAST_LOAD_START, stat -> stat.value);
            final LocalDate lastUpdate = LocalDate.parse(lastLoadDt);
            this.startDt = parametersService.startCtlParameter().orElse(lastUpdate.minusMonths(1).withDayOfMonth(1));
            this.filterEcosysDzoClientAggDay = col("d2").geq(startDt)
                    .and(col("d2").lt(buildDate()))
                    .and(col("ctl_loading").gt(lastLoadId));
            this.filterEcosysTxnDet = col("txn_dt").geq(startDt)
                    .and(col("txn_dt").lt(buildDate()))
                    .and(col("ctl_loading").gt(lastLoadId));
        }
    }

    @Override
    public Dataset<Row> buildDatamart() {
        final Dataset<Row> tgtTable;

        final Dataset<Row> dimEcosysDzoClientAggDay = sourceTable(DepfinTables.DIM_ECOSYS_DZO_CLIENT_AGG_DAY)
                .where(filterEcosysDzoClientAggDay);
        Dataset<Row> dzoUser = getDzoUser(dimEcosysDzoClientAggDay, "dzo_user");
        Dataset<Row> dzoLast90DaysUser = getDzoUser(dimEcosysDzoClientAggDay, true, "dzo_user_90days");

        final Dataset<Row> ftEcosysTxnDet = sourceTable(DepfinTables.FT_ECOSYS_TXN_DET)
                .where(filterEcosysTxnDet);
        Dataset<Row> competitorTxnUser = getCompetitorTxnUser(ftEcosysTxnDet, "competitor_user");
        Dataset<Row> competitorTxnLast90DaysUser = getCompetitorTxnUser(ftEcosysTxnDet, true, "competitor_user_90days");

        // определение клиентов с регулярными транзакциями
        final String srcSchemaClientAgg = SysPropertyTool.getSystemProperty(
                "spark.source.schema_client_aggr", "prx_hdp2_client_aggr_custom_rozn_client_aggr");
        Dataset<Row> clientAggrMnth = sourceTable(FullTableName.of(srcSchemaClientAgg, "ft_client_aggr_mnth"));

        Dataset<Row> clientAggrMnthActual = clientAggrMnth
                .where(col("report_dt").equalTo(buildDate().minusMonths(1).withDayOfMonth(
                        buildDate().minusMonths(1).lengthOfMonth())));

        if (clientAggrMnthActual.isEmpty()) {
            clientAggrMnthActual = clientAggrMnth
                    .where(col("report_dt").equalTo(buildDate().minusMonths(2).withDayOfMonth(
                            buildDate().minusMonths(2).lengthOfMonth())));
        }

        if (clientAggrMnthActual.isEmpty()) {
            throw new IllegalStateException(
                    String.format("Table %s.ft_client_aggr_mnth does not contain data for the last 2 months.", srcSchemaClientAgg));
        }

        clientAggrMnthActual = clientAggrMnthActual
                .select(col("epk_id"),
                        col("seg_client_fl_segment_cd"),
                        col("report_dt"));

        Dataset<Row> mvsSegRegTxnEgroceryClient = getRegularTxnClient(
                clientAggrMnthActual, ftEcosysTxnDet, MVS_SEGMENT, EGROCERY_PARTNER_TYPE_ID, "mvs_seg_egrocery");
        saveTemp(mvsSegRegTxnEgroceryClient, "mvs_seg_reg_txn_egrocery_flag");

        Dataset<Row> mvsSegRegTxnEcomClient = getRegularTxnClient(
                clientAggrMnthActual, ftEcosysTxnDet, MVS_SEGMENT, ECOM_PARTNER_TYPE_ID, "mvs_seg_ecom");
        saveTemp(mvsSegRegTxnEcomClient, "mvs_seg_reg_txn_ecom_flag");

        Dataset<Row> ageSegRegTxnEgroceryClient = getRegularTxnClient(
                clientAggrMnthActual, ftEcosysTxnDet, AGE_SEGMENT, EGROCERY_PARTNER_TYPE_ID, "age_seg_egrocery");
        saveTemp(ageSegRegTxnEgroceryClient, "age_seg_reg_txn_egrocery_flag");

        Dataset<Row> ageSegRegTxnEcomClient = getRegularTxnClient(
                clientAggrMnthActual, ftEcosysTxnDet, AGE_SEGMENT, ECOM_PARTNER_TYPE_ID, "age_seg_ecom");
        saveTemp(ageSegRegTxnEcomClient, "age_seg_reg_txn_ecom_flag");

        Dataset<Row> allEpk = clientAggrMnthActual.select(col("epk_id"))
                .unionAll(dzoUser.select(col("epk_id")))
                .distinct();

        Dataset<Row> dzoClientParam = allEpk
                .join(dzoUser,
                        allEpk.col("epk_id").equalTo(dzoUser.col("epk_id")), "left")
                .join(dzoLast90DaysUser,
                        allEpk.col("epk_id").equalTo(dzoLast90DaysUser.col("epk_id")), "left")
                .join(competitorTxnUser,
                        allEpk.col("epk_id").equalTo(competitorTxnUser.col("epk_id")), "left")
                .join(competitorTxnLast90DaysUser,
                        allEpk.col("epk_id").equalTo(competitorTxnLast90DaysUser.col("epk_id")), "left")
                .join(mvsSegRegTxnEgroceryClient,
                        allEpk.col("epk_id").equalTo(mvsSegRegTxnEgroceryClient.col("epk_id")), "left")
                .join(mvsSegRegTxnEcomClient,
                        allEpk.col("epk_id").equalTo(mvsSegRegTxnEcomClient.col("epk_id")), "left")
                .join(ageSegRegTxnEgroceryClient,
                        allEpk.col("epk_id").equalTo(ageSegRegTxnEgroceryClient.col("epk_id")), "left")
                .join(ageSegRegTxnEcomClient,
                        allEpk.col("epk_id").equalTo(ageSegRegTxnEcomClient.col("epk_id")), "left")
                .select(
                        allEpk.col("epk_id").as("epk_id"),
                        coalesce(dzoUser.col("okko_user"), lit(0)).as("okko_user"),
                        coalesce(dzoUser.col("zvuk_user"), lit(0)).as("zvuk_user"),
                        coalesce(dzoUser.col("megamarket_user"), lit(0)).as("megamarket_user"),

                        coalesce(dzoLast90DaysUser.col("okko_user"), lit(0)).as("okko_user_90days"),
                        coalesce(dzoLast90DaysUser.col("zvuk_user"), lit(0)).as("zvuk_user_90days"),
                        coalesce(dzoLast90DaysUser.col("megamarket_user"), lit(0)).as("megamarket_user_90days"),

                        coalesce(competitorTxnUser.col("okko_competitor_user"), lit(0)).as("okko_competitor_user"),
                        coalesce(competitorTxnUser.col("zvuk_competitor_user"), lit(0)).as("zvuk_competitor_user"),
                        coalesce(competitorTxnLast90DaysUser.col("okko_competitor_user"), lit(0)).as("okko_competitor_user_90days"),
                        coalesce(competitorTxnLast90DaysUser.col("zvuk_competitor_user"), lit(0)).as("zvuk_competitor_user_90days"),

                        coalesce(mvsSegRegTxnEgroceryClient.col("reg_txn"), lit(0)).as("mvs_seg_reg_txn_egrocery"),
                        coalesce(mvsSegRegTxnEcomClient.col("reg_txn"), lit(0)).as("mvs_seg_reg_txn_ecom"),
                        coalesce(ageSegRegTxnEgroceryClient.col("reg_txn"), lit(0)).as("age_seg_reg_txn_egrocery"),
                        coalesce(ageSegRegTxnEcomClient.col("reg_txn"), lit(0)).as("age_seg_reg_txn_ecom")
                )
                .withColumn("row_hash",
                        sha2(concat(
                                coalesce(col("epk_id").cast(StringType), lit("NULL")),
                                coalesce(col("okko_user").cast(StringType), lit("NULL")),
                                coalesce(col("zvuk_user").cast(StringType), lit("NULL")),
                                coalesce(col("megamarket_user").cast(StringType), lit("NULL")),
                                coalesce(col("okko_user_90days").cast(StringType), lit("NULL")),
                                coalesce(col("zvuk_user_90days").cast(StringType), lit("NULL")),
                                coalesce(col("megamarket_user_90days").cast(StringType), lit("NULL")),
                                coalesce(col("okko_competitor_user").cast(StringType), lit("NULL")),
                                coalesce(col("zvuk_competitor_user").cast(StringType), lit("NULL")),
                                coalesce(col("okko_competitor_user_90days").cast(StringType), lit("NULL")),
                                coalesce(col("zvuk_competitor_user_90days").cast(StringType), lit("NULL")),
                                coalesce(col("mvs_seg_reg_txn_egrocery").cast(StringType), lit("NULL")),
                                coalesce(col("mvs_seg_reg_txn_ecom").cast(StringType), lit("NULL")),
                                coalesce(col("age_seg_reg_txn_egrocery").cast(StringType), lit("NULL")),
                                coalesce(col("age_seg_reg_txn_ecom").cast(StringType), lit("NULL"))
                        ), 256))
                .withColumn("row_actual_from_dt", lit(buildDate().minusDays(1)).cast(StringType));

        if (isFirstLoading) {
            tgtTable = dzoClientParam
                    .withColumn("row_actual_to_dt", lit(ACTUAL_TO_DT))
                    .where(col("epk_id").notEqual(-1));
        } else {
            Dataset<Row> newActual = saveTemp(dzoClientParam, "new_for_inc");
            Dataset<Row> oldActual = sourceTable(FullTableName.of("custom_rb_depfin.scd_dzo_client_agg_param"))
                    .where(col("row_actual_to_dt").equalTo(ACTUAL_TO_DT))
                    .drop(col("row_actual_to_dt"))
                    .drop(col("ctl_loading"))
                    .drop(col("ctl_validfrom"))
                    .drop(col("ctl_action"));

            Dataset<Row> joinDf = oldActual.join(newActual, oldActual.col("epk_id")
                    .equalTo(newActual.col("epk_id")), "full");

            // версионность - неизмененные записи
            Dataset<Row> resultNoChanges = saveTemp(joinDf
                    .where(oldActual.col("row_hash").equalTo(newActual.col("row_hash")))
                    .select(oldActual.col("*"),
                            lit(ACTUAL_TO_DT).as("row_actual_to_dt")), "result_no_changes");

            // версионность - актуальные записи (новые или измененные)
            Dataset<Row> resultNew = saveTemp(joinDf
                    .where(coalesce(oldActual.col("row_hash"), lit("null")).notEqual(newActual.col("row_hash"))
                            .and(newActual.col("row_hash").isNotNull()))
                    .select(newActual.col("*"),
                            lit(ACTUAL_TO_DT).as("row_actual_to_dt")), "result_new");

            // версионность - исторические записи
            Dataset<Row> resultHist = saveTemp(joinDf
                    .where(oldActual.col("row_hash").notEqual(newActual.col("row_hash"))
                            .and(oldActual.col("row_hash").isNotNull()))
                    .select(oldActual.col("*"),
                            (date_add(newActual.col("row_actual_from_dt"), -1)).cast(StringType)
                                    .as("row_actual_to_dt")), "result_hist");

            // если за один день поток запускался > 1 раза, то берется последняя актуальная запись
            final WindowSpec windowForActualFrom = Window.partitionBy(col("epk_id"), col("row_actual_from_dt"))
                    .orderBy(col("row_actual_to_dt").desc());

            final Dataset<Row> newClientParamResUnion = unionAll(resultNoChanges, resultNew, resultHist)
                    .withColumn("row_num", row_number().over(windowForActualFrom))
                    .where(col("row_num").equalTo(1));

            final Dataset<Row> newClientParamRes = newClientParamResUnion
                    .drop(col("row_num"));

            // обновляем партиции, по которым пришли изменения
            final Dataset<Row> partitionsForUpdate = newClientParamRes
                    .select(col("row_actual_to_dt"))
                    .distinct();

            final Dataset<Row> oldClientParam = sourceTable(FullTableName.of("custom_rb_depfin.scd_dzo_client_agg_param"))
                    .where(col("row_actual_to_dt").notEqual("9999-12-31"));

            final Dataset<Row> oldHistoryRows = oldClientParam
                    .join(partitionsForUpdate, oldClientParam.col("row_actual_to_dt").equalTo(partitionsForUpdate.col("row_actual_to_dt")), "inner")
                    .drop(partitionsForUpdate.col("row_actual_to_dt"))
                    .drop(oldClientParam.col("ctl_loading"))
                    .drop(oldClientParam.col("ctl_validfrom"))
                    .drop(oldClientParam.col("ctl_action"));

            tgtTable = unionAll(oldHistoryRows, newClientParamRes)
                    .where(col("epk_id").notEqual(-1));
        }

        String maxTxnLoading = Optional.ofNullable(getAggColumnValue(ftEcosysTxnDet, "ctl_loading", functions::max))
                .orElse(lastLoadId).toString();
        String maxDzoLoading = Optional.ofNullable(getAggColumnValue(dimEcosysDzoClientAggDay, "ctl_loading", functions::max))
                .orElse(lastLoadId).toString();
        String minLoading = maxTxnLoading.compareTo(maxDzoLoading) > 0 ? maxDzoLoading : maxTxnLoading;

        String maxTxnCtlLoadingDt = Optional.ofNullable(getAggColumnValue(ftEcosysTxnDet, "txn_dt", functions::max))
                .orElse(lastLoadDt).toString();
        String maxDzoCtlLoadingDt = Optional.ofNullable(getAggColumnValue(dimEcosysDzoClientAggDay, "d2", functions::max))
                .orElse(lastLoadDt).toString();
        String minCtlLoadingDt = maxTxnCtlLoadingDt.compareTo(maxDzoCtlLoadingDt) > 0 ? maxDzoCtlLoadingDt : maxTxnCtlLoadingDt;

        addStatistic(PROCESSED_LOADING_ID, minLoading);
        addStatistic(LAST_LOAD_START, minCtlLoadingDt);

        return tgtTable;
    }

    private Dataset<Row> getDzoUserFlag(Dataset<Row> dzoClientAggDay) {
        Dataset<Row> dzoUser = dzoClientAggDay
                .where(col("partner_id").isin(OKKO_ID, ZVUK_ID, MEGAMARKET_ID))
                .groupBy("epk_id")
                .agg(
                        max(when(col("partner_id").equalTo(OKKO_ID)
                                .and(col("evt_id").isin(OKKO_USER_EVT_ID)), 1).otherwise(0)).as("okko_user"),
                        max(when(col("partner_id").equalTo(OKKO_ID)
                                .and(col("evt_id").isin(OKKO_USER_EVT_ID)), col("d2"))).as("okko_user_dt"),
                        max(when(col("partner_id").equalTo(ZVUK_ID)
                                .and(col("evt_id").isin(ZVUK_USER_EVT_ID)), 1).otherwise(0)).as("zvuk_user"),
                        max(when(col("partner_id").equalTo(ZVUK_ID)
                                .and(col("evt_id").isin(ZVUK_USER_EVT_ID)), col("d2"))).as("zvuk_user_dt"),
                        max(when(col("partner_id").equalTo(MEGAMARKET_ID)
                                .and(col("evt_id").isin(MEGAMARKET_USER_EVT_ID)), 1).otherwise(0)).as("megamarket_user"),
                        max(when(col("partner_id").equalTo(MEGAMARKET_ID)
                                .and(col("evt_id").isin(MEGAMARKET_USER_EVT_ID)), col("d2"))).as("megamarket_user_dt"))
                .select(
                        col("epk_id"),
                        col("okko_user"),
                        col("okko_user_dt"),
                        col("zvuk_user"),
                        col("zvuk_user_dt"),
                        col("megamarket_user"),
                        col("megamarket_user_dt")
                );

        return dzoUser;
    }

    private Dataset<Row> getDzoUser(Dataset<Row> dzoClientAggDay, boolean last90Days, String tempTableName) {
        Dataset<Row> dzoUser;
        final Column filterDzoCLientAggDay;
        final String minDate;

        if (last90Days) {
            minDate = buildDate().minusDays(90).toString();
            filterDzoCLientAggDay = col("d2").geq(minDate);
        } else {
            minDate = DEFAULT_START_DT.toString();
            filterDzoCLientAggDay = lit("1").equalTo(lit("1"));
        }

        final Dataset<Row> dzoClientAggDayFiltered = dzoClientAggDay.where(filterDzoCLientAggDay);
        final Dataset<Row> newDzoUserFlag = getDzoUserFlag(dzoClientAggDayFiltered);

        if (!isFirstLoading) {
            final Dataset<Row> oldDzoUserFlag = sourceTable(FullTableName.of(
                    "custom_rb_depfin_stg", "scd_dzo_client_agg_param_" + tempTableName));
            final Dataset<Row> dzoUserUnionOldAndNewAgg = oldDzoUserFlag.unionAll(newDzoUserFlag)
                    .groupBy("epk_id")
                    .agg(
                            max("okko_user").as("okko_user"),
                            max("okko_user_dt").as("okko_user_dt"),
                            max("zvuk_user").as("zvuk_user"),
                            max("zvuk_user_dt").as("zvuk_user_dt"),
                            max("megamarket_user").as("megamarket_user"),
                            max("megamarket_user_dt").as("megamarket_user_dt")
                    )
                    .select(
                            col("epk_id").as("epk_id"),
                            col("okko_user"),
                            col("okko_user_dt"),
                            col("zvuk_user"),
                            col("zvuk_user_dt"),
                            col("megamarket_user"),
                            col("megamarket_user_dt"));

            dzoUser = dzoUserUnionOldAndNewAgg
                    .select(
                            col("epk_id").as("epk_id"),
                            when(col("okko_user_dt").geq(minDate), col("okko_user"))
                                    .otherwise(0).as("okko_user"),
                            col("okko_user_dt"),
                            when(col("zvuk_user_dt").geq(minDate), col("zvuk_user"))
                                    .otherwise(0).as("zvuk_user"),
                            col("zvuk_user_dt"),
                            when(col("megamarket_user_dt").geq(minDate), col("megamarket_user"))
                                    .otherwise(0).as("megamarket_user"),
                            col("megamarket_user_dt"));
        } else {
            dzoUser = newDzoUserFlag;
        }

        saveTemp(dzoUser.select(
                col("epk_id").as("epk_id"),
                col("okko_user").as("okko_user"),
                col("okko_user_dt").as("okko_user_dt"),
                col("zvuk_user").as("zvuk_user"),
                col("zvuk_user_dt").as("zvuk_user_dt"),
                col("megamarket_user").as("megamarket_user"),
                col("megamarket_user_dt").as("megamarket_user_dt")), tempTableName + "_temp");

        dzoUser = saveTemp(sourceTable(FullTableName.of("custom_rb_depfin_stg",
                        String.format("scd_dzo_client_agg_param_%s_temp", tempTableName))),
                tempTableName);

        return dzoUser
                .select(
                        col("epk_id").as("epk_id"),
                        col("okko_user").as("okko_user"),
                        col("zvuk_user").as("zvuk_user"),
                        col("megamarket_user").as("megamarket_user")
                );
    }

    private Dataset<Row> getDzoUser(Dataset<Row> dzoClientAggDay, String tempTableName) {
        return getDzoUser(dzoClientAggDay, false, tempTableName);
    }

    private Dataset<Row> getCompetitorTxnUserFlag(Dataset<Row> txnDet) {
        Dataset<Row> competitorTxnUser = txnDet
                .where(col("partner_type_id").isin(ONLINE_CINEMA_PARTNER_TYPE_ID, MUSIC_PARTNER_TYPE_ID)
                        .and(col("sber_eco_flag").notEqual(1)))
                .groupBy("epk_id")
                .agg(
                        max(when(col("partner_type_id").equalTo(ONLINE_CINEMA_PARTNER_TYPE_ID), 1)
                                .otherwise(0)).as("okko_competitor_user"),
                        max(when(col("partner_type_id").equalTo(ONLINE_CINEMA_PARTNER_TYPE_ID),
                                col("txn_dt"))).as("okko_competitor_user_dt"),
                        max(when(col("partner_type_id").equalTo(MUSIC_PARTNER_TYPE_ID), 1)
                                .otherwise(0)).as("zvuk_competitor_user"),
                        max(when(col("partner_type_id").equalTo(MUSIC_PARTNER_TYPE_ID),
                                col("txn_dt"))).as("zvuk_competitor_user_dt")
                )
                .select(
                        col("epk_id"),
                        col("okko_competitor_user"),
                        col("okko_competitor_user_dt"),
                        col("zvuk_competitor_user"),
                        col("zvuk_competitor_user_dt")
                );

        return competitorTxnUser;
    }

    private Dataset<Row> getCompetitorTxnUser(Dataset<Row> txnDet, boolean last90Days, String tempTableName) {
        Dataset<Row> competitorTxnUser;
        final Column filterTxnDet;
        final String minDate;

        if (last90Days) {
            minDate = buildDate().minusDays(90).toString();
            filterTxnDet = col("txn_dt").geq(minDate);

        } else {
            filterTxnDet = lit("1").equalTo(lit("1"));
            minDate = DEFAULT_START_DT.toString();
        }

        final Dataset<Row> txnDetFiltered = txnDet.where(filterTxnDet);
        final Dataset<Row> newCompetitorTxnUser = getCompetitorTxnUserFlag(txnDetFiltered);

        if (!isFirstLoading) {
            final Dataset<Row> oldCompetitorTxnUser = sourceTable(FullTableName.of(
                    "custom_rb_depfin_stg", "scd_dzo_client_agg_param_" + tempTableName));

            final Dataset<Row> competitorTxnUserUnionOldAndNew = oldCompetitorTxnUser.unionAll(newCompetitorTxnUser)
                    .groupBy("epk_id")
                    .agg(
                            max("okko_competitor_user").as("okko_competitor_user"),
                            max("okko_competitor_user_dt").as("okko_competitor_user_dt"),
                            max("zvuk_competitor_user").as("zvuk_competitor_user"),
                            max("zvuk_competitor_user_dt").as("zvuk_competitor_user_dt")
                    )
                    .select(
                            col("epk_id").as("epk_id"),
                            col("okko_competitor_user"),
                            col("okko_competitor_user_dt"),
                            col("zvuk_competitor_user"),
                            col("zvuk_competitor_user_dt"));

            competitorTxnUser = competitorTxnUserUnionOldAndNew
                    .select(
                            col("epk_id").as("epk_id"),
                            when(col("okko_competitor_user_dt").geq(minDate), col("okko_competitor_user"))
                                    .otherwise(0).as("okko_competitor_user"),
                            col("okko_competitor_user_dt"),
                            when(col("zvuk_competitor_user_dt").geq(minDate), col("zvuk_competitor_user"))
                                    .otherwise(0).as("zvuk_competitor_user"),
                            col("zvuk_competitor_user_dt"));
        } else {
            competitorTxnUser = newCompetitorTxnUser;
        }

        saveTemp(competitorTxnUser.select(
                col("epk_id").as("epk_id"),
                col("okko_competitor_user").as("okko_competitor_user"),
                col("okko_competitor_user_dt").as("okko_competitor_user_dt"),
                col("zvuk_competitor_user").as("zvuk_competitor_user"),
                col("zvuk_competitor_user_dt").as("zvuk_competitor_user_dt")), tempTableName + "_temp");

        competitorTxnUser = saveTemp(sourceTable(
                        FullTableName.of("custom_rb_depfin_stg",
                                String.format("scd_dzo_client_agg_param_%s_temp", tempTableName))),
                tempTableName);

        return competitorTxnUser
                .select(
                        col("epk_id").as("epk_id"),
                        col("okko_competitor_user").as("okko_competitor_user"),
                        col("zvuk_competitor_user").as("zvuk_competitor_user")
                );
    }

    private Dataset<Row> getCompetitorTxnUser(Dataset<Row> txnDet, String tempTableName) {
        return getCompetitorTxnUser(txnDet, false, tempTableName);
    }

    private Dataset<Row> getRegularTxnClient(Dataset<Row> clientAggr, Dataset<Row> txnDet, Object[] segment, Object[] partnerTypeId, String tempTableName) {
        Dataset<Row> regTxn;
        String minDate = buildDate().minusDays(SAVED_INTERVAL).toString();

        final Dataset<Row> clientAggrSegment = clientAggr
                .where(col("seg_client_fl_segment_cd").isin(segment));

        Dataset<Row> newTxn = txnDet
                .where(col("partner_type_id").isin(partnerTypeId)
                        .and(col("txn_dt").geq(buildDate().minusDays(SAVED_INTERVAL))))
                .join(clientAggrSegment, txnDet.col("epk_id").equalTo(clientAggrSegment.col("epk_id")))
                .groupBy(clientAggrSegment.col("epk_id"))
                .agg(sort_array(collect_set(col("txn_dt"))).as("txn"))
                .select(
                        clientAggrSegment.col("epk_id").as("epk_id"),
                        col("txn")
                );

        if (!isFirstLoading) {

            final Dataset<Row> oldTxn = sourceTable(FullTableName.of(
                    "custom_rb_depfin_stg", String.format("scd_dzo_client_agg_param_cl_txn_%s", tempTableName)));

            regTxn = oldTxn
                    .join(newTxn, oldTxn.col("epk_id").equalTo(newTxn.col("epk_id")), "outer")
                    .withColumn("txn_union", sort_array(array_distinct(array_union(
                            coalesce(oldTxn.col("txn"), array()),
                            coalesce(newTxn.col("txn"), array())
                    ))))
                    .select(
                            coalesce(oldTxn.col("epk_id"), newTxn.col("epk_id")).as("epk_id"),
                            filter(col("txn_union"), date ->
                                    date.isNotNull().and(date.geq(minDate))).as("txn")
                    );
        } else {
            regTxn = newTxn;
        }

        regTxn = regTxn
                .where(size(col("txn")).geq(4));

        saveTemp(regTxn, String.format("cl_txn_%s_temp", tempTableName));

        regTxn = saveTemp(
                sourceTable(FullTableName.of("custom_rb_depfin_stg",
                        String.format("scd_dzo_client_agg_param_cl_txn_%s_temp", tempTableName))),
                String.format("cl_txn_%s", tempTableName));

        JavaRDD<Row> regTxnClient = regTxn.toJavaRDD().map(new GetRegularTxnFlag());

        return dc.context().createDataFrame(regTxnClient, REGULAR_TXN_CLIENT_STRUCT);
    }
}