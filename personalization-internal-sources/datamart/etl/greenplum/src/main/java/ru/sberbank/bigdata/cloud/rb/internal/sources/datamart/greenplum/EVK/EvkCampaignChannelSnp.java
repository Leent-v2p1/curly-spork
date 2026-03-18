package ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.greenplum.EVK;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.DatamartRef;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.PartialReplace;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.DatamartServiceFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.Datamart;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.hive.PartitionInfo;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_remover.UpdatedPartitionRemover;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_strategy.HiveSavingStrategy;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_strategy.PartitionedSavingStrategy;
import ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.greenplum.EvkTables;

import java.text.SimpleDateFormat;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.AutoConfigDatamartRunner.runner;

@PartialReplace(partitioning = "date_build_snp", saveRemover = UpdatedPartitionRemover.class)
@DatamartRef(id = "custom_rb_evk.ref_union_campaign_channel_snp", name = "Snapshot ref_union_campaign_channel_snp")
public class EvkCampaignChannelSnp extends Datamart {
    private static final String DATE_BUILD_SNP = new SimpleDateFormat("yyyy-MM-dd").format(System.currentTimeMillis());

    public static void main(String[] args) {
        runner().run(EvkCampaignChannelSnp.class);
    }

    @Override
    public HiveSavingStrategy customSavingStrategy(DatamartServiceFactory datamartServiceFactory) {
        final PartitionInfo triggersPartitioning = PartitionInfo.dynamic().add("date_build_snp").create();
        return new PartitionedSavingStrategy(triggersPartitioning);
    }

    @Override
    public Dataset<Row> buildDatamart() {
        final Dataset<Row> evk = sourceTable(EvkTables.REF_UNION_CAMPAIGN_CHANNEL);
        return evk.select(
                col("channel_id").cast(LongType),
                col("channel_name").cast(StringType),
                col("channel_type").cast(StringType),
                col("channel_name_desc").cast(StringType),
                col("channel_system").cast(StringType)
        ).withColumn("date_build_snp", lit(DATE_BUILD_SNP).cast(StringType));
    }
}