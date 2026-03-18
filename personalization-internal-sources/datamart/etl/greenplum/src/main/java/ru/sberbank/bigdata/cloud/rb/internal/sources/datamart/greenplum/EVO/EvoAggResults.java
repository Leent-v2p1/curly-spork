package ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.greenplum.EVO;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.DatamartRef;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.FullReplace;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.Datamart;

import java.util.Optional;

import org.apache.spark.sql.types.DateType;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.*;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.AutoConfigDatamartRunner.runner;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.DecimalTypes.DECIMAL_18_4;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId.PROCESSED_LOADING_ID;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.greenplum.GreenplumConstants.*;

@DatamartRef(id = "custom_rozn_evo.da_evo_agg_results_hdp", name = "Ref evo agg results hdp", useSystemPropertyToGetId = true)
@FullReplace
public class EvoAggResults extends Datamart {

    private static final Logger log = LoggerFactory.getLogger(EvoAggResults.class);

    @Override
    public Dataset<Row> buildDatamart() {

        SparkSession spark = SparkSession
                .builder()
                .appName("greenplum_stg_evo_agg_results_hdp")
                .enableHiveSupport()
                .getOrCreate();

        String gpurl = Optional.of(spark.conf().get("spark.jdbc.gpurl")).orElse(GP_URL);
        String user = Optional.of(spark.conf().get("spark.jdbc.gpUser")).orElse(GP_USER);
        String partitionColumn = Optional.of(spark.conf().get("spark.jdbc.partitionColumn")).orElse(GP_PARTITION_COLUMN);

        log.info("gpUrl: {}, gpuser: {}, partitionColumn: {}", gpurl, user, partitionColumn);

        Dataset<Row> sourceTable = spark.read().format("io.pivotal.greenplum.spark.GreenplumRelationProvider")
                .option("dbschema", "s_grnplm_vd_rozn_mpp_aaas_vd")
                .option("dbtable", "da_evo_agg_results_hdp")
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
                        col("ab_test_id").cast(LongType).as("ab_test_id"),
                        col("add_npv").cast(DECIMAL_18_4).as("add_npv"),
                        col("add_npv_clear").cast(DECIMAL_18_4).as("add_npv_clear"),
                        col("add_npv_clear_per_member").cast(DECIMAL_18_4).as("add_npv_clear_per_member"),
                        col("add_resp").cast(FloatType).as("add_resp"),
                        col("add_resp_cnt").cast(DECIMAL_18_4).as("add_resp_cnt"),
                        col("add_resp_mnth").cast(FloatType).as("add_resp_mnth"),
                        col("add_resp_mnth_cnt").cast(DECIMAL_18_4).as("add_resp_mnth_cnt"),
                        col("add_target_qty").cast(DECIMAL_18_4).as("add_target_qty"),
                        col("add_target_vl").cast(DECIMAL_18_4).as("add_target_vl"),
                        col("additional_metrics").cast(StringType).as("additional_metrics"),
                        col("cg_cnt").cast(IntegerType).as("cg_cnt"),
                        col("cg_npv").cast(DECIMAL_18_4).as("cg_npv"),
                        col("cg_resp").cast(FloatType).as("cg_resp"),
                        col("cg_resp_cnt").cast(IntegerType).as("cg_resp_cnt"),
                        col("cg_resp_mnth").cast(FloatType).as("cg_resp_mnth"),
                        col("cg_resp_mnth_cnt").cast(IntegerType).as("cg_resp_mnth_cnt"),
                        col("cg_target_qty").cast(DECIMAL_18_4).as("cg_target_qty"),
                        col("cg_target_vl").cast(DECIMAL_18_4).as("cg_target_vl"),
                        col("changed_ts").cast(TimestampType).as("changed_ts"),
                        col("changed_user").cast(StringType).as("changed_user"),
                        col("comm_cost").cast(DECIMAL_18_4).as("comm_cost"),
                        col("cost_by_chnl_json").cast(StringType).as("cost_by_chnl_json"),
                        col("rule_sk_id").cast(IntegerType).as("rule_sk_id"),
                        col("sign_flag_resp").cast(IntegerType).as("sign_flag_resp"),
                        col("sign_flag_target_qty").cast(IntegerType).as("sign_flag_target_qty"),
                        col("sign_flag_target_vl").cast(IntegerType).as("sign_flag_target_vl"),
                        col("t_stat_resp").cast(FloatType).as("t_stat_resp"),
                        col("t_stat_resp_mnth").cast(FloatType).as("t_stat_resp_mnth"),
                        col("t_stat_target_qty").cast(FloatType).as("t_stat_target_qty"),
                        col("t_stat_target_vl").cast(FloatType).as("t_stat_target_vl"),
                        col("tg_cnt").cast(IntegerType).as("tg_cnt"),
                        col("tg_npv").cast(DECIMAL_18_4).as("tg_npv"),
                        col("tg_resp").cast(FloatType).as("tg_resp"),
                        col("tg_resp_cnt").cast(IntegerType).as("tg_resp_cnt"),
                        col("tg_resp_mnth").cast(FloatType).as("tg_resp_mnth"),
                        col("tg_resp_mnth_cnt").cast(IntegerType).as("tg_resp_mnth_cnt"),
                        col("tg_target_qty").cast(DECIMAL_18_4).as("tg_target_qty"),
                        col("tg_target_vl").cast(DECIMAL_18_4).as("tg_target_vl"),
                        col("unique_only_nflag").cast(IntegerType).as("unique_only_nflag")
                );

        return result;
    }

    public static void main(String[] args) {
        runner().run(EvoAggResults.class);
    }
}