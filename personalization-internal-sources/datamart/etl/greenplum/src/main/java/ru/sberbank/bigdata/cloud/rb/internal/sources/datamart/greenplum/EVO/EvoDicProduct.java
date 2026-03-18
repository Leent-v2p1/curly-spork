package ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.greenplum.EVO;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.DatamartRef;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.FullReplace;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.DatamartServiceFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.ParametersService;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.Datamart;

import java.sql.Date;
import java.util.Optional;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.types.DataTypes.*;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.AutoConfigDatamartRunner.runner;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId.PROCESSED_LOADING_ID;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.greenplum.GreenplumConstants.*;

@DatamartRef(id = "custom_rozn_evo.da_dic_product_hdp", name = "Ref dic product", useSystemPropertyToGetId = true)
@FullReplace
public class EvoDicProduct extends Datamart {

    private static final Logger log = LoggerFactory.getLogger(EvoDicProduct.class);

    @Override
    public Dataset<Row> buildDatamart() {

        SparkSession spark = SparkSession
                .builder()
                .appName("greenplum_stg_ref_da_dic_product_hdp")
                .enableHiveSupport()
                .getOrCreate();

        String gpurl = Optional.of(spark.conf().get("spark.jdbc.gpurl")).orElse(GP_URL);
        String user = Optional.of(spark.conf().get("spark.jdbc.gpUser")).orElse(GP_USER);
        String partitionColumn = Optional.of(spark.conf().get("spark.jdbc.partitionColumn")).orElse(GP_PARTITION_COLUMN);

        log.info("gpurl: {}, gpuser: {}, partitionColumn: {}", gpurl, user, partitionColumn);

        Dataset<Row> sourceTable = spark.read().format("io.pivotal.greenplum.spark.GreenplumRelationProvider")
                .option("dbschema", "s_grnplm_vd_rozn_mpp_aaas_vd")
                .option("dbtable", "da_dic_product_hdp")
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
                        col("c_product_id").cast(IntegerType).as("c_product_id"),
                        col("c_product_lv1").cast(StringType).as("c_product_lv1"),
                        col("c_product_lv2").cast(StringType).as("c_product_lv2"),
                        col("c_product_lv3").cast(StringType).as("c_product_lv3"),
                        col("c_product_lv4").cast(StringType).as("c_product_lv4"),
                        col("c_product_lv5").cast(StringType).as("c_product_lv5"),
                        col("initial_crm_product_id").cast(StringType).as("initial_crm_product_id"),
                        col("initial_crm_product_name").cast(StringType).as("initial_crm_product_name"),
                        col("squad").cast(StringType).as("squad"),
                        col("timestamp_change").cast(TimestampType).as("timestamp_change"),
                        col("product_group_sale_link").cast(StringType).as("product_group_sale_link"),
                        col("product_group_sale_link_id").cast(IntegerType).as("product_group_sale_link_id"),
                        col("c_product_lv1_id").cast(IntegerType).as("c_product_lv1_id"),
                        col("c_product_lv2_id").cast(IntegerType).as("c_product_lv2_id"),
                        col("c_product_lv3_id").cast(IntegerType).as("c_product_lv3_id"),
                        col("c_product_lv4_id").cast(IntegerType).as("c_product_lv4_id"),
                        col("c_product_lv5_id").cast(IntegerType).as("c_product_lv5_id"),
                        col("product_group").cast(StringType).as("product_group"),
                        col("partner_id").cast(IntegerType).as("partner_id"),
                        col("cr_product").cast(StringType).as("cr_product"),
                        col("cr_product_group").cast(StringType).as("cr_product_group"),
                        col("product_resp_flag").cast(IntegerType).as("product_resp_flag"),
                        col("open_resp_flag").cast(IntegerType).as("open_resp_flag")
                );
        return result;
    }

    public static void main(String[] args) {
        runner().run(EvoDicProduct.class);
    }
}