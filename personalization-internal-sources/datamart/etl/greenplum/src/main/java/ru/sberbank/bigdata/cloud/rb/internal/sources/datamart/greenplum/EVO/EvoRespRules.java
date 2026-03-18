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

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.types.DataTypes.*;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.AutoConfigDatamartRunner.runner;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId.PROCESSED_LOADING_ID;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.greenplum.GreenplumConstants.*;

@DatamartRef(id = "custom_rozn_evo.da_evo_resp_rules_hdp", name = "Ref da_evo_resp_rules_hdp", useSystemPropertyToGetId = true)
@FullReplace
public class EvoRespRules extends Datamart {

    private static final Logger log = LoggerFactory.getLogger(EvoRespRules.class);

    @Override
    public Dataset<Row> buildDatamart() {
        SparkSession spark = SparkSession
                .builder()
                .appName("greenplum_stg_evo_resp_rules_hdp")
                .enableHiveSupport()
                .getOrCreate();

        final String gpurl = Optional.of(spark.conf().get("spark.jdbc.gpurl")).orElse(GP_URL);
        final String user = Optional.of(spark.conf().get("spark.jdbc.gpUser")).orElse(GP_USER);
        final String partitionColumn = Optional.of(spark.conf().get("spark.jdbc.partitionColumn")).orElse(GP_PARTITION_COLUMN);
        final int partitions = Optional.of(Integer.parseInt(spark.conf().get("spark.jdbc.partitions"))).orElse(GP_PARTITIONS);

        log.info("gpUrl: {}, gpuser: {}, partitionColumn: {}", gpurl, user, partitionColumn);

        Dataset<Row> sourceTable = spark.read().format("io.pivotal.greenplum.spark.GreenplumRelationProvider")
                .option("dbschema", "s_grnplm_vd_rozn_mpp_aaas_vd")
                .option("dbtable", "da_evo_resp_rules_hdp")
                .option("url", gpurl)
                .option("user", user)
                .option("driver", "org.postgresql.Driver")
                .option("pool.maxSize", "5")
                .option("server.nic", "eth1")
                .option("partitions", partitions)
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
                        col("sale_type_id").cast(IntegerType).as("sale_type_id"),
                        col("sale_product_class_id").cast(IntegerType).as("sale_product_class_id"),
                        col("cond_has_add_params_nflag").cast(IntegerType).as("cond_has_add_params_nflag"),
                        col("ext_src_rule_id").cast(IntegerType).as("ext_src_rule_id"),
                        col("sk_id").cast(IntegerType).as("sk_id"),
                        col("univers_add_params").cast(StringType).as("univers_add_params"),
                        lit(null).cast(StringType).as("camp_report_dt"),
                        col("calc_type_id").cast(StringType).as("calc_type_id"),
                        lit(null).cast(StringType).as("mp_kpi_nflag"),
                        col("calc_resp_nflag").cast(StringType).as("calc_resp_nflag"),
                        col("resp_start_dt").cast(StringType).as("resp_start_dt"),
                        col("resp_end_dt").cast(StringType).as("resp_end_dt"),
                        lit(null).cast(StringType).as("priority"),
                        col("priority_order").cast(StringType).as("priority_order"),
                        col("must_have_cg_nflag").cast(StringType).as("must_have_cg_nflag"),
                        col("condition_text").cast(StringType).as("condition_text"),
                        col("qualify_cond_left_op").cast(StringType).as("qualify_cond_left_op"),
                        col("qualify_cond_right_op").cast(StringType).as("qualify_cond_right_op"),
                        col("response_dt").cast(StringType).as("response_dt"),
                        col("host_agrmnt_id").cast(StringType).as("host_agrmnt_id"),
                        col("npv").cast(StringType).as("npv"),
                        col("target_vl").cast(StringType).as("target_vl"),
                        col("mapp_params").cast(StringType).as("mapp_params"),
                        col("vnv_partition_dyn").cast(StringType).as("vnv_partition_dyn"),
                        col("evk_hist_condition_text").cast(StringType).as("evk_hist_condition_text"),
                        col("changed_ts").cast(TimestampType).as("changed_ts"),
                        col("rule_desc").cast(StringType).as("rule_desc"),
                        col("changed_user").cast(StringType).as("changed_user")
                );
        return result;
    }
    public static void main(String[] args) {
        runner().run(EvoRespRules.class);
    }
}