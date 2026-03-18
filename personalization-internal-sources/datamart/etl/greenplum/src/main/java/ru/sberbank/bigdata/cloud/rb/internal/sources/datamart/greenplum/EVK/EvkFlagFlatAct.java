package ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.greenplum.EVK;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.DatamartRef;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.PartialReplace;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.DatamartServiceFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.ParametersService;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.Datamart;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.hive.PartitionInfo;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_remover.UpdatedPartitionRemover;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_strategy.HiveSavingStrategy;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_strategy.PartitionedSavingStrategy;

import java.sql.Date;
import java.util.Optional;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.types.DataTypes.*;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.AutoConfigDatamartRunner.runner;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId.PROCESSED_LOADING_ID;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.greenplum.GreenplumConstants.*;

@DatamartRef(id = "custom_rb_evk.ft_nba_pkd_flag_flat_act", name = "ft_nba_pkd_flag_flat_act", useSystemPropertyToGetId = true)
@PartialReplace(partitioning = "report_dt", saveRemover = UpdatedPartitionRemover.class)
public class EvkFlagFlatAct extends Datamart {

    private static final Logger log = LoggerFactory.getLogger(EvkFlagFlatAct.class);
    private Date endDt;
    private Date startDt;

    @Override
    public void init(DatamartServiceFactory datamartServiceFactory) {
        final ParametersService parametersService = datamartServiceFactory.parametersService();
        this.endDt = Date.valueOf(parametersService.endCtlParameter().orElse(buildDate()));
        this.startDt = Date.valueOf(parametersService.startCtlParameter().orElse(buildDate().minusDays(2)));
    }

    @Override
    public HiveSavingStrategy customSavingStrategy(DatamartServiceFactory datamartServiceFactory) {
        return new PartitionedSavingStrategy(PartitionInfo.dynamic().add("report_dt").create());
    }

    @Override
    public Dataset<Row> buildDatamart() {

        SparkSession spark = SparkSession
                .builder()
                .appName("greenplum_stg_billing_local_ufo")
                .enableHiveSupport()
                .getOrCreate();

        String gpurl = Optional.of(spark.conf().get("spark.jdbc.gpurl")).orElse(GP_URL);
        String user = Optional.of(spark.conf().get("spark.jdbc.gpUser")).orElse(GP_USER);
        String partitionColumn = Optional.of(spark.conf().get("spark.jdbc.partitionColumn")).orElse(GP_PARTITION_COLUMN);

        log.info("gpUrl: {}, gpuser: {}, partitionColumn: {}", gpurl, user, partitionColumn);

        Dataset<Row> sourceTable = spark.read().format("io.pivotal.greenplum.spark.GreenplumRelationProvider")
                .option("dbschema", "s_grnplm_vd_rozn_mpp_daas_vd")
                .option("dbtable", "ft_nba_pkd_flag_flat_act")
                .option("url", gpurl)
                .option("user", user)
                .option("driver", "org.postgresql.Driver")
                .option("pool.maxSize", "5")
                .option("server.nic", "eth1")
                .option("partitionColumn", partitionColumn)
                .load();

        addStatistic(PROCESSED_LOADING_ID, buildDate().toString());
        disableDefaultStatistics();

        final Column filter = col("report_dt").gt(startDt).and(col("report_dt").leq(endDt));

        Dataset<Row> result = sourceTable
                .where(filter)
                .select(
                        col("report_dt").cast(StringType),
                        col("epk_id"),
                        col("cmpn_sms_avail_nflag_no_sl").cast(IntegerType),
                        col("cmpn_sms_avail_nflag").cast(IntegerType),
                        col("cmpn_push_sbol_avail_nflag_no_sl").cast(IntegerType),
                        col("cmpn_push_sbol_avail_nflag").cast(IntegerType),
                        col("cmpn_push_sms_avail_nflag_no_sl").cast(IntegerType),
                        col("cmpn_push_sms_avail_nflag").cast(IntegerType),
                        col("cmpn_tm_avail_nflag_no_sl").cast(IntegerType),
                        col("cmpn_tm_avail_nflag").cast(IntegerType),
                        col("cmpn_tm_robot_avail_nflag_no_sl").cast(IntegerType),
                        col("cmpn_tm_robot_avail_nflag").cast(IntegerType),
                        col("cmpn_push_sberinvestor_avail_nflag_no_sl").cast(IntegerType),
                        col("cmpn_push_sberinvestor_avail_nflag").cast(IntegerType),
                        col("cmpn_email_avail_nflag_no_sl").cast(IntegerType),
                        col("cmpn_email_avail_nflag").cast(IntegerType),
                        col("cmpn_channel_erkc_ivr_in_avail_nflag_no_sl").cast(IntegerType),
                        col("cmpn_channel_erkc_ivr_in_avail_nflag").cast(IntegerType),
                        col("cmpn_channel_sitesber_avail_nflag_no_sl").cast(IntegerType),
                        col("cmpn_channel_sitesber_avail_nflag").cast(IntegerType),
                        col("cmpn_channel_sbolpro_avail_nflag_no_sl").cast(IntegerType),
                        col("cmpn_channel_sbolpro_avail_nflag").cast(IntegerType),
                        col("cmpn_channel_kmpremierpull_avail_nflag_no_sl").cast(IntegerType),
                        col("cmpn_channel_kmpremierpull_avail_nflag").cast(IntegerType),
                        col("cmpn_banner_mpsbol_avail_nflag_no_sl").cast(IntegerType),
                        col("cmpn_banner_mpsbol_avail_nflag").cast(IntegerType),
                        col("cmpn_banner_sberinvestor_avail_nflag_no_sl").cast(IntegerType),
                        col("cmpn_banner_sberinvestor_avail_nflag").cast(IntegerType),
                        col("cmpn_banner_websbol_avail_nflag_no_sl").cast(IntegerType),
                        col("cmpn_banner_websbol_avail_nflag").cast(IntegerType),
                        col("cmpn_dsa_avail_nflag_no_sl").cast(IntegerType),
                        col("cmpn_dsa_avail_nflag").cast(IntegerType),
                        col("cmpn_dkm_avail_nflag_no_sl").cast(IntegerType),
                        col("cmpn_dkm_avail_nflag").cast(IntegerType),
                        col("cmpn_sberads_avail_nflag_no_sl").cast(IntegerType),
                        col("cmpn_sberads_avail_nflag").cast(IntegerType),
                        col("cmpn_va_avail_nflag_no_sl").cast(IntegerType),
                        col("cmpn_va_avail_nflag").cast(IntegerType),
                        col("cmpn_push_km_sb1_avail_nflag_no_sl").cast(IntegerType),
                        col("cmpn_push_km_sb1_avail_nflag").cast(IntegerType),
                        col("cmpn_channel_kmsb1pull_avail_nflag_no_sl").cast(IntegerType),
                        col("cmpn_channel_kmsb1pull_avail_nflag").cast(IntegerType),
                        col("cmpn_atm_avail_nflag").cast(IntegerType),
                        col("cmpn_atm_avail_nflag_no_sl").cast(IntegerType),
                        col("cmpn_km_b2b_avail_nflag").cast(IntegerType),
                        col("cmpn_km_b2b_avail_nflag_no_sl").cast(IntegerType),
                        col("cmpn_push_km_pb_avail_nflag").cast(IntegerType),
                        col("cmpn_push_km_pb_avail_nflag_no_sl").cast(IntegerType),
                        col("cmpn_push_km_premier_avail_nflag").cast(IntegerType),
                        col("cmpn_push_km_premier_avail_nflag_no_sl").cast(IntegerType),
                        col("cmpn_sb_bol_avail_nflag").cast(IntegerType),
                        col("cmpn_sb_bol_avail_nflag_no_sl").cast(IntegerType),
                        col("cmpn_sber_id_avail_nflag").cast(IntegerType),
                        col("cmpn_sber_id_avail_nflag_no_sl").cast(IntegerType)
                );

        return result;
    }

    public static void main(String[] args) {
        runner().run(EvkFlagFlatAct.class);
    }
}