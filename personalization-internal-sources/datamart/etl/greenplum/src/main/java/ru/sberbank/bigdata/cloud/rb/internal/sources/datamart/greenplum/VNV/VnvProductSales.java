package ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.greenplum.VNV;

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

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.types.DataTypes.*;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.AutoConfigDatamartRunner.runner;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.DecimalTypes.DECIMAL_38_26;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId.PROCESSED_LOADING_ID;

@DatamartRef(id = "custom_rb_vnv.ft_prod_sales", name = "ProdSales", useSystemPropertyToGetId = true)
@PartialReplace(partitioning = "sale_dt", saveRemover = UpdatedPartitionRemover.class)

public class VnvProductSales extends Datamart {
    private static final Logger log = LoggerFactory.getLogger(VnvProductSales.class);
    private String gpurl;
    private String user;
    private String partitionColumn;
    private Date endDt;
    private Date startDt;

    @Override
    public HiveSavingStrategy customSavingStrategy(DatamartServiceFactory datamartServiceFactory) {
        return new PartitionedSavingStrategy(PartitionInfo.dynamic().add("sale_dt").create());
    }

    @Override
    public void init(DatamartServiceFactory datamartServiceFactory) {
        final ParametersService parametersService = datamartServiceFactory.parametersService();
        this.endDt = Date.valueOf(parametersService.endCtlParameter().orElse(buildDate()));
        this.startDt = Date.valueOf(parametersService.startCtlParameter()
                .orElse(buildDate().minusMonths(1).withDayOfMonth(1)));
    }

    @Override
    public Dataset<Row> buildDatamart() {

        SparkSession spark = SparkSession
                .builder()
                .appName("greenplum_stg_prod_sales")
                .enableHiveSupport()
                .getOrCreate();

        gpurl = spark.conf().get("spark.jdbc.gpurl");
        user = spark.conf().get("spark.jdbc.gpUser");
        partitionColumn = spark.conf().get("spark.jdbc.partitionColumn");

        log.info("gpUrl: {}, gpuser: {}, partitionColumn: {}", gpurl, user, partitionColumn);

        Dataset<Row> ProdSales = spark.read().format("io.pivotal.greenplum.spark.GreenplumRelationProvider")
                .option("dbschema", "s_grnplm_vd_rozn_mpp_aaas_vd")
                .option("dbtable", "ft_prod_sales_hdp")
                .option("url", gpurl)
                .option("user", user)
                .option("driver", "org.postgresql.Driver")
                .option("pool.maxSize", "5")
                .option("server.nic", "eth1")
                .option("partitionColumn", partitionColumn)
                .load();

        addStatistic(PROCESSED_LOADING_ID, buildDate().toString());
        disableDefaultStatistics();

        final Column filter = isFirstLoading
                ? lit("1").equalTo(lit("1"))
                : col("sale_dt").gt(startDt).and(col("sale_dt").leq(endDt));

        Dataset<Row> result = ProdSales
                .where(filter)
                .select(
                        col("sale_sk").cast(StringType).as("sale_sk"),
                        col("sale_dt").cast(StringType).as("sale_dt"),
                        col("sale_type_id").cast(IntegerType).as("sale_type_id"),
                        col("sale_product_class_id").cast(IntegerType).as("sale_product_class_id"),
                        col("sale_product_group1_name").cast(StringType).as("sale_product_group1_name"),
                        col("sale_product_group2_name").cast(StringType).as("sale_product_group2_name"),
                        col("sale_product_group3_name").cast(StringType).as("sale_product_group3_name"),
                        col("host_agrmnt_id").cast(LongType).as("host_agrmnt_id"),
                        col("orig_agrmnt_exp_dt").cast(StringType).as("orig_agrmnt_exp_dt"),
                        col("epk_id").cast(LongType).as("epk_id"),
                        col("insert_ts").cast(TimestampType).as("insert_ts"),
                        col("npv_avg").cast(DECIMAL_38_26).as("npv_avg"),
                        col("extra_json").cast(StringType).as("extra_json"),
                        col("product_class_name").cast(StringType).as("product_class_name"),
                        col("sale_type_name").cast(StringType).as("sale_type_name"),
                        col("sale_channel_name").cast(StringType).as("sale_channel_name")
                );

        return result;
    }

    public static void main(String[] args) {
        runner().run(VnvProductSales.class);
    }
}
