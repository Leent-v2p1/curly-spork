package ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.greenplum.EVO;

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
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.DecimalTypes.DECIMAL_18_4;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId.PROCESSED_LOADING_ID;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.greenplum.GreenplumConstants.*;

@DatamartRef(id = "custom_rozn_evo.da_evo_monthly_results_hdp", name = "Ref da_evo_monthly_results_hdp", useSystemPropertyToGetId = true)
@PartialReplace(partitioning = "resp_month", saveRemover = UpdatedPartitionRemover.class)
public class EvoMonthlyResults extends Datamart {

    private static final Logger log = LoggerFactory.getLogger(EvoMonthlyResults.class);

    @Override
    public HiveSavingStrategy customSavingStrategy(DatamartServiceFactory datamartServiceFactory) {
        return new PartitionedSavingStrategy(PartitionInfo.dynamic().add("resp_month").create());
    }

    @Override
    public Dataset<Row> buildDatamart() {

        SparkSession spark = SparkSession
                .builder()
                .appName("greenplum_stg_evo_monthly_results_hdp")
                .enableHiveSupport()
                .getOrCreate();

        String gpurl = Optional.of(spark.conf().get("spark.jdbc.gpurl")).orElse(GP_URL);
        String user = Optional.of(spark.conf().get("spark.jdbc.gpUser")).orElse(GP_USER);
        String partitionColumn = Optional.of(spark.conf().get("spark.jdbc.partitionColumn")).orElse(GP_PARTITION_COLUMN);

        log.info("gpUrl: {}, gpuser: {}, partitionColumn: {}", gpurl, user, partitionColumn);

        Dataset<Row> sourceTable = spark.read().format("io.pivotal.greenplum.spark.GreenplumRelationProvider")
                .option("dbschema", "s_grnplm_vd_rozn_mpp_aaas_vd")
                .option("dbtable", "da_evo_monthly_results_hdp")
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
                        col("resp_month").cast(StringType).as("resp_month"),
                        col("t_stat_target_vl").cast(FloatType).as("t_stat_target_vl"),
                        col("t_stat_target_qty").cast(FloatType).as("t_stat_target_qty"),
                        col("tg_resp").cast(FloatType).as("tg_resp"),
                        col("cg_resp").cast(FloatType).as("cg_resp"),
                        col("add_resp").cast(FloatType).as("add_resp"),
                        col("t_stat_resp").cast(FloatType).as("t_stat_resp"),
                        col("unique_only_nflag").cast(IntegerType).as("unique_only_nflag"),
                        col("sign_flag_target_vl").cast(IntegerType).as("sign_flag_target_vl"),
                        col("sign_flag_target_qty").cast(IntegerType).as("sign_flag_target_qty"),
                        col("sign_flag_resp").cast(IntegerType).as("sign_flag_resp"),
                        col("rule_sk_id").cast(IntegerType).as("rule_sk_id"),
                        col("tg_cnt").cast(IntegerType).as("tg_cnt"),
                        col("cg_cnt").cast(IntegerType).as("cg_cnt"),
                        col("tg_resp_cnt").cast(IntegerType).as("tg_resp_cnt"),
                        col("cg_resp_cnt").cast(IntegerType).as("cg_resp_cnt"),
                        col("ab_test_id").cast(LongType).as("ab_test_id"),
                        col("cost_by_chnl_json").cast(StringType).as("cost_by_chnl_json"),
                        col("additional_metrics").cast(StringType).as("additional_metrics"),
                        col("tg_target_vl").cast(DECIMAL_18_4).as("tg_target_vl"),
                        col("cg_target_vl").cast(DECIMAL_18_4).as("cg_target_vl"),
                        col("add_target_vl").cast(DECIMAL_18_4).as("add_target_vl"),
                        col("tg_npv").cast(DECIMAL_18_4).as("tg_npv"),
                        col("cg_npv").cast(DECIMAL_18_4).as("cg_npv"),
                        col("add_npv").cast(DECIMAL_18_4).as("add_npv"),
                        col("comm_cost").cast(DECIMAL_18_4).as("comm_cost"),
                        col("add_npv_clear").cast(DECIMAL_18_4).as("add_npv_clear"),
                        col("add_npv_clear_per_member").cast(DECIMAL_18_4).as("add_npv_clear_per_member"),
                        col("tg_target_qty").cast(DECIMAL_18_4).as("tg_target_qty"),
                        col("cg_target_qty").cast(DECIMAL_18_4).as("cg_target_qty"),
                        col("add_target_qty").cast(DECIMAL_18_4).as("add_target_qty"),
                        col("add_resp_cnt").cast(DECIMAL_18_4).as("add_resp_cnt"),
                        col("changed_ts").cast(TimestampType).as("changed_ts"),
                        col("changed_user").cast(StringType).as("changed_user")
                );

        return result;
    }

    public static void main(String[] args) {
        runner().run(EvoMonthlyResults.class);
    }
}