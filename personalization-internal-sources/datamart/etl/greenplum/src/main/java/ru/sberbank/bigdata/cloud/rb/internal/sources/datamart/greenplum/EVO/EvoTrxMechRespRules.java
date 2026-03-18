package ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.greenplum.EVO;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.DatamartRef;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.FullReplace;
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
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.AutoConfigDatamartRunner.runner;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId.PROCESSED_LOADING_ID;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.greenplum.GreenplumConstants.*;

@DatamartRef(id = "custom_rozn_evo.da_evo_trx_mech_resp_rules_hdp", name = "Ref da_evo_trx_mech_resp_rules_hdp", useSystemPropertyToGetId = true)
@FullReplace
public class EvoTrxMechRespRules extends Datamart {

    private static final Logger log = LoggerFactory.getLogger(EvoTrxMechRespRules.class);

    @Override
    public Dataset<Row> buildDatamart() {

        SparkSession spark = SparkSession
                .builder()
                .appName("greenplum_stg_evo_trx_mech_resp_rules_hdp")
                .enableHiveSupport()
                .getOrCreate();

        String gpurl = Optional.of(spark.conf().get("spark.jdbc.gpurl")).orElse(GP_URL);
        String user = Optional.of(spark.conf().get("spark.jdbc.gpUser")).orElse(GP_USER);
        String partitionColumn = Optional.of(spark.conf().get("spark.jdbc.partitionColumn")).orElse(GP_PARTITION_COLUMN);

        log.info("gpUrl: {}, gpuser: {}, partitionColumn: {}", gpurl, user, partitionColumn);

        Dataset<Row> sourceTable = spark.read().format("io.pivotal.greenplum.spark.GreenplumRelationProvider")
                .option("dbschema", "s_grnplm_vd_rozn_mpp_aaas_vd")
                .option("dbtable", "da_evo_trx_mech_resp_rules_hdp")
                .option("url", gpurl)
                .option("user", user)
                .option("driver", "org.postgresql.Driver")
                .option("pool.maxSize", "5")
                .option("server.nic", "eth1")
                .option("partitionColumn", partitionColumn)
                .load();

        addStatistic(PROCESSED_LOADING_ID, buildDate().toString());
        disableDefaultStatistics();

        Dataset<Row> result = sourceTable
                .select(
                        col("row_actual_from_dt").cast(DateType).as("row_actual_from_dt"),
                        col("row_actual_to_dt").cast(DateType).as("row_actual_to_dt"),
                        col("rule_id").cast(IntegerType).as("rule_id"),
                        col("response_type_id").cast(IntegerType).as("response_type_id"),
                        col("resp_stage").cast(IntegerType).as("resp_stage"),
                        col("sk_id").cast(IntegerType).as("sk_id"),
                        col("camp_report_dt").cast(StringType).as("camp_report_dt"),
                        col("calc_type_id").cast(StringType).as("calc_type_id"),
                        col("mp_kpi_nflag").cast(StringType).as("mp_kpi_nflag"),
                        col("calc_resp_nflag").cast(StringType).as("calc_resp_nflag"),
                        col("must_have_cg_nflag").cast(StringType).as("must_have_cg_nflag"),
                        col("resp_start_dt").cast(StringType).as("resp_start_dt"),
                        col("resp_end_dt").cast(StringType).as("resp_end_dt"),
                        col("resp_start_dt_before").cast(StringType).as("resp_start_dt_before"),
                        col("resp_end_dt_before").cast(StringType).as("resp_end_dt_before"),
                        col("cards_scope").cast(StringType).as("cards_scope"),
                        col("trx_scope").cast(StringType).as("trx_scope"),
                        col("perso_param").cast(StringType).as("perso_param"),
                        col("common_param").cast(StringType).as("common_param"),
                        col("bonus_resp_start_dt").cast(StringType).as("bonus_resp_start_dt"),
                        col("bonus_resp_end_dt").cast(StringType).as("bonus_resp_end_dt"),
                        col("bonus_card_scope").cast(StringType).as("bonus_card_scope"),
                        col("bonus_txn_scope").cast(StringType).as("bonus_txn_scope"),
                        col("bonus_calc").cast(StringType).as("bonus_calc"),
                        col("changed_ts").cast(TimestampType).as("changed_ts"),
                        col("rule_desc").cast(StringType).as("rule_desc"),
                        col("changed_user").cast(StringType).as("changed_user")
                );
        return result;
    }

    public static void main(String[] args) {
        runner().run(EvoTrxMechRespRules.class);
    }
}