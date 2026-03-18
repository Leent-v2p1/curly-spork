package ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.greenplum.EKP;

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

@DatamartRef(id = "custom_rb_loan.v_is0188_ekp_dic_product", name = "Ref v_is0188_ekp_dic_product", useSystemPropertyToGetId = true)
@FullReplace
public class EkpDicProduct extends Datamart {
    private static final Logger log = LoggerFactory.getLogger(EkpDicProduct.class);


    @Override
    public void init(DatamartServiceFactory datamartServiceFactory) {
        final ParametersService parametersService = datamartServiceFactory.parametersService();
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
                .option("dbschema", "s_grnplm_vd_rozn_mpp_daas_bf_vd")
                .option("dbtable", "v_is0188_ekp_dic_product")
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
                        col("host_product_id").cast(LongType).as("host_product_id"),
                        col("product_id").cast(LongType).as("product_id"),
                        col("product_det_id").cast(IntegerType).as("product_det_id"),
                        col("group_id").cast(IntegerType).as("group_id"),
                        col("is_collateral").cast(IntegerType).as("is_collateral"),
                        col("motrgage_market").cast(StringType).as("motrgage_market"),
                        col("type_motrgage_product").cast(StringType).as("type_motrgage_product"),
                        col("group_name").cast(StringType).as("group_name"),
                        col("product_name_client").cast(StringType).as("product_name_client"),
                        col("product_name").cast(StringType).as("product_name"),
                        col("product_code").cast(StringType).as("product_code"),
                        col("product_type_cd").cast(StringType).as("product_type_cd")
                );

        return result;
    }

    public static void main(String[] args) {
        runner().run(EkpDicProduct.class);
    }
}