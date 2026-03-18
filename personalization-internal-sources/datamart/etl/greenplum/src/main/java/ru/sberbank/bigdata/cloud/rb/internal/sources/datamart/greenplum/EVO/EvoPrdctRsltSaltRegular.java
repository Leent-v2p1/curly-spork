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
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.DecimalTypes.DECIMAL_18_6;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId.PROCESSED_LOADING_ID;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.greenplum.GreenplumConstants.*;

@DatamartRef(id = "custom_rozn_evo.da_prdct_rslt_salt_regular_hdp", name = "Ref da_prdct_rslt_salt_regular_hdp", useSystemPropertyToGetId = true)
@PartialReplace(partitioning = "row_actual_to_dt", saveRemover = UpdatedPartitionRemover.class)
public class EvoPrdctRsltSaltRegular extends Datamart {

    private static final Logger log = LoggerFactory.getLogger(EvoPrdctRsltSaltRegular.class);

    @Override
    public HiveSavingStrategy customSavingStrategy(DatamartServiceFactory datamartServiceFactory) {
        return new PartitionedSavingStrategy(PartitionInfo.dynamic().add("row_actual_to_dt").create());
    }

    @Override
    public Dataset<Row> buildDatamart() {

        SparkSession spark = SparkSession
                .builder()
                .appName("greenplum_stg_prdct_rslt_salt_regular_hdp")
                .enableHiveSupport()
                .getOrCreate();

        String gpurl = Optional.of(spark.conf().get("spark.jdbc.gpurl")).orElse(GP_URL);
        String user = Optional.of(spark.conf().get("spark.jdbc.gpUser")).orElse(GP_USER);
        String partitionColumn = Optional.of(spark.conf().get("spark.jdbc.partitionColumn")).orElse(GP_PARTITION_COLUMN);

        log.info("gpUrl: {}, gpuser: {}, partitionColumn: {}", gpurl, user, partitionColumn);

        Dataset<Row> sourceTable = spark.read().format("io.pivotal.greenplum.spark.GreenplumRelationProvider")
                .option("dbschema", "s_grnplm_vd_rozn_mpp_aaas_vd")
                .option("dbtable", "da_prdct_rslt_salt_regular_hdp")
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
                        col("row_actual_to_dt").cast(StringType).as("row_actual_to_dt"),
                        col("prod_cat_link_prdct_id").cast(IntegerType).as("prod_cat_link_prdct_id"),
                        col("prod_cat_link_sl_type").cast(IntegerType).as("prod_cat_link_sl_type"),
                        col("range_from").cast(IntegerType).as("range_from"),
                        col("range_to").cast(IntegerType).as("range_to"),
                        col("insert_ts").cast(TimestampType).as("insert_ts"),
                        col("prdct_id").cast(StringType).as("prdct_id"),
                        col("prdct_name").cast(StringType).as("prdct_name"),
                        col("sale_type_id").cast(StringType).as("sale_type_id"),
                        col("sale_type_name").cast(StringType).as("sale_type_name"),
                        col("salt_pattern").cast(StringType).as("salt_pattern"),
                        col("dflt_evk_product_id").cast(StringType).as("dflt_evk_product_id"),
                        col("product_name").cast(StringType).as("product_name"),
                        col("insert_username").cast(StringType).as("insert_username")
                );
        return result;
    }

    public static void main(String[] args) {
        runner().run(EvoPrdctRsltSaltRegular.class);
    }
}