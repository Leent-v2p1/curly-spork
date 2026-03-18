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
import static org.apache.spark.sql.types.DataTypes.DateType;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.AutoConfigDatamartRunner.runner;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId.PROCESSED_LOADING_ID;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.greenplum.GreenplumConstants.*;

@DatamartRef(id = "custom_rozn_evo.da_evo_dic_hdp", name = "Ref da_evo_dic_hdp", useSystemPropertyToGetId = true)
@FullReplace
public class EvoDic extends Datamart {

    private static final Logger log = LoggerFactory.getLogger(EvoDic.class);

    @Override
    public Dataset<Row> buildDatamart() {

        SparkSession spark = SparkSession
                .builder()
                .appName("greenplum_stg_evo_dic_hdp")
                .enableHiveSupport()
                .getOrCreate();

        String gpurl = Optional.of(spark.conf().get("spark.jdbc.gpurl")).orElse(GP_URL);
        String user = Optional.of(spark.conf().get("spark.jdbc.gpUser")).orElse(GP_USER);
        String partitionColumn = Optional.of(spark.conf().get("spark.jdbc.partitionColumn")).orElse(GP_PARTITION_COLUMN);

        log.info("gpUrl: {}, gpuser: {}, partitionColumn: {}", gpurl, user, partitionColumn);

        Dataset<Row> sourceTable = spark.read().format("io.pivotal.greenplum.spark.GreenplumRelationProvider")
                .option("dbschema", "s_grnplm_vd_rozn_mpp_aaas_vd")
                .option("dbtable", "da_evo_dic_hdp")
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
                        col("rule_sk_id").cast(IntegerType).as("rule_sk_id"),
                        col("camp_start_dt").cast(DateType).as("camp_start_dt"),
                        col("camp_end_dt").cast(DateType).as("camp_end_dt"),
                        col("resp_start_dt").cast(DateType).as("resp_start_dt"),
                        col("resp_end_dt").cast(DateType).as("resp_end_dt"),
                        col("camp_report_dt").cast(DateType).as("camp_report_dt"),
                        col("campaign_code").cast(StringType).as("campaign_code"),
                        col("rnk").cast(IntegerType).as("rnk"),
                        col("campaign_name").cast(StringType).as("campaign_name"),
                        col("campaign_name_cf").cast(StringType).as("campaign_name_cf"),
                        col("campaign_type").cast(StringType).as("campaign_type"),
                        col("segment").cast(StringType).as("segment"),
                        col("unique_segment").cast(StringType).as("unique_segment"),
                        col("product_id").cast(StringType).as("product_id"),
                        col("product_name").cast(StringType).as("product_name"),
                        col("product_group_name").cast(StringType).as("product_group_name"),
                        col("product_sgroup_name").cast(StringType).as("product_sgroup_name"),
                        col("product_group").cast(StringType).as("product_group"),
                        col("product_group_sale_link_id").cast(IntegerType).as("product_group_sale_link_id"),
                        col("channels_list").cast(StringType).as("channels_list"),
                        col("sources_list").cast(StringType).as("sources_list"),
                        col("source_processes_list").cast(StringType).as("source_processes_list"),
                        col("start_dt").cast(DateType).as("start_dt"),
                        col("end_dt").cast(DateType).as("end_dt"),
                        col("wave_launch_dt").cast(DateType).as("wave_launch_dt"),
                        col("wave_deactivation_dt").cast(DateType).as("wave_deactivation_dt"),
                        col("mp_kpi_nflag").cast(IntegerType).as("mp_kpi_nflag"),
                        col("must_have_cg_nflag").cast(IntegerType).as("must_have_cg_nflag"),
                        col("rule_source").cast(StringType).as("rule_source"),
                        col("rule_id").cast(IntegerType).as("rule_id"),
                        col("rule_source_id").cast(IntegerType).as("rule_source_id"),
                        col("calc_type_id").cast(IntegerType).as("calc_type_id"),
                        col("add_params").cast(StringType).as("add_params"),
                        col("changed_ts").cast(TimestampType).as("changed_ts"),
                        col("changed_user").cast(StringType).as("changed_user")
                );

        return result;
    }

    public static void main(String[] args) {
        runner().run(EvoDic.class);
    }
}