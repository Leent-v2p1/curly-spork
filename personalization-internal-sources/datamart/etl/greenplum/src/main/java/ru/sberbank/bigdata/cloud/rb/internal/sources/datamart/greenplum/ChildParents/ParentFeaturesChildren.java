package ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.greenplum.ChildParents;

import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;
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
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.DecimalTypes.DECIMAL_20_10;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.AutoConfigDatamartRunner.runner;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId.PROCESSED_LOADING_ID;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.greenplum.GreenplumConstants.*;

@DatamartRef(id = "custom_rozn_family.dm_parent_feature_children", name = "Ref dm_parent_feature_children", useSystemPropertyToGetId = true)
@PartialReplace(partitioning = "row_actual_to_dt", saveRemover = UpdatedPartitionRemover.class)
public class ParentFeaturesChildren extends Datamart {
    private static final Logger log = LoggerFactory.getLogger(ParentFeaturesChildren.class);

    @Override
    public HiveSavingStrategy customSavingStrategy(DatamartServiceFactory datamartServiceFactory) {
        return new PartitionedSavingStrategy(PartitionInfo.dynamic().add("row_actual_to_dt").create());
    }

    @Override
    public Dataset<Row> buildDatamart() {
        SparkSession spark = SparkSession
                .builder()
                .appName("greenplum_stg_custom_rozn_family.dm_parent_feature_children")
                .enableHiveSupport()
                .getOrCreate();

        String gpurl = Optional.of(spark.conf().get("spark.jdbc.gpurl")).orElse(GP_URL);
        String user = Optional.of(spark.conf().get("spark.jdbc.gpUser")).orElse(GP_USER);
        String partitionColumn = Optional.of(spark.conf().get("spark.jdbc.partitionColumn")).orElse(GP_PARTITION_COLUMN);
        log.info("gpUrl: {}, gpuser: {}, partitionColumn: {}", gpurl, user, partitionColumn);

        Dataset<Row> ParentFeaturesChildren = spark.read().format("io.pivotal.greenplum.spark.GreenplumRelationProvider")
                .option("dbschema", "s_grnplm_vd_rozn_mpp_daas_hdp_vd")
                .option("dbtable", "dm_parent_feature_children")
                .option("url", gpurl)
                .option("user", user)
                .option("driver", "org.postgresql.Driver")
                .option("pool.maxSize", "5")
                .option("server.nic", "eth1")
                .option("partitionColumn", partitionColumn)
                .load();

        addStatistic(PROCESSED_LOADING_ID, buildDate().toString());
        disableDefaultStatistics();

        Dataset<Row> sourceTable = ParentFeaturesChildren
                .select(
                        col("row_actual_flg").cast(BooleanType).as("row_actual_flg"),
                        col("row_actual_from_dt").cast(DateType).as("row_actual_from_dt"),
                        col("row_actual_to_dt").cast(StringType).as("row_actual_to_dt"),
                        col("birth_dt").cast(DateType).as("birth_dt"),
                        col("twin_num").cast(IntegerType).as("twin_num"),
                        col("child_epk_id").cast(LongType).as("child_epk_id"),
                        col("parent_epk_id").cast(LongType).as("parent_epk_id"),
                        col("weight_prc").cast(DECIMAL_20_10).as("weight_prc"),
                        col("source_cd").cast(StringType).as("source_cd")
                );
        return sourceTable;
    }

    public static void main(String[] args) {
        runner().run(ParentFeaturesChildren.class);
    }
}
