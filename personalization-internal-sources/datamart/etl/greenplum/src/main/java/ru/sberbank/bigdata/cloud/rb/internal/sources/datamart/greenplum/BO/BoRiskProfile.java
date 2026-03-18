package ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.greenplum.BO;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.DatamartRef;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.FullReplace;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.DatamartServiceFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.Datamart;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.hive.PartitionInfo;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_strategy.HiveSavingStrategy;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_strategy.PartitionedSavingStrategy;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.AutoConfigDatamartRunner.runner;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId.PROCESSED_LOADING_ID;

@DatamartRef(id = "custom_rb_bo.v_is0121_bo_risk_profile", name = "Bo Risk Profile", useSystemPropertyToGetId = true)
@FullReplace
public class BoRiskProfile extends Datamart {

    private static final Logger log = LoggerFactory.getLogger(BoRiskProfile.class);
    private String gpurl;
    private String user;
    private String partitionColumn;

    @Override
    public Dataset<Row> buildDatamart() {

        SparkSession spark = SparkSession
                .builder()
                .appName("greenplum_stg_bo_risk_profile")
                .enableHiveSupport()
                .getOrCreate();

        gpurl = spark.conf().get("spark.jdbc.gpurl");
        user = spark.conf().get("spark.jdbc.gpUser");
        partitionColumn = spark.conf().get("spark.jdbc.partitionColumn");

        log.info("gpUrl: {}, gpuser: {}, partitionColumn: {}", gpurl, user, partitionColumn);

        Dataset<Row> dicAgreement = spark.read().format("io.pivotal.greenplum.spark.GreenplumRelationProvider")
                .option("dbschema", "s_grnplm_vd_rozn_mpp_daas_bf_vd")
                .option("dbtable", "v_is0121_bo_risk_profile")
                .option("url", gpurl)
                .option("user", user)
                .option("driver", "org.postgresql.Driver")
                .option("pool.maxSize", "5")
                .option("server.nic", "eth1")
                .option("partitionColumn", partitionColumn)
                .load();

        addStatistic(PROCESSED_LOADING_ID, buildDate().toString());
        disableDefaultStatistics();

        Dataset<Row> result = dicAgreement
                .select(
                        col("tb_name"),
                        col("start_dt").cast(StringType).as("start_dt"),
                        col("staff_fio"),
                        col("staff_crm_id"),
                        col("risk_status"),
                        col("qistatus"),
                        col("qidate").cast(StringType).as("qidate"),
                        col("profile_id"),
                        col("order_status_date").cast(StringType).as("order_status_date"),
                        col("ntb").cast(IntegerType).as("ntb"),
                        col("manager_fio"),
                        col("manager_crm_id"),
                        col("info_system_id").cast(IntegerType).as("info_system_id"),
                        col("epk_id"),
                        col("end_dt").cast(StringType).as("end_dt"),
                        col("date_add").cast(StringType).as("date_add"),
                        col("branch_num"),
                        col("branch_name"),
                        col("agrmnt_id"),
                        col("actual_version").cast(IntegerType).as("actual_version")
                );

        return result;
    }

    public static void main(String[] args) {
        runner().run(BoRiskProfile.class);
    }
}
