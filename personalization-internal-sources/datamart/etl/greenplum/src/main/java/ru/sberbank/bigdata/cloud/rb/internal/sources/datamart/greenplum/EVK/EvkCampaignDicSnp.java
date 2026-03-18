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
import static org.apache.spark.sql.types.DataTypes.StringType;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.AutoConfigDatamartRunner.runner;


@PartialReplace(partitioning = "date_build_snp", saveRemover = UpdatedPartitionRemover.class)
@DatamartRef(id = "custom_rb_evk.ref_union_campaign_dic_hdp_snp", name = "Snapshot ref_union_campaign_dic_hdp")
public class EvkCampaignDicSnp extends Datamart {
    private static final String DATE_BUILD_SNP = new SimpleDateFormat("yyyy-MM-dd").format(System.currentTimeMillis());

    @Override
    public HiveSavingStrategy customSavingStrategy(DatamartServiceFactory datamartServiceFactory) {
        final PartitionInfo triggersPartitioning = PartitionInfo.dynamic().add("date_build_snp").create();
        return new PartitionedSavingStrategy(triggersPartitioning);
    }

    @Override
    public Dataset<Row> buildDatamart() {
        final Dataset<Row> evk = sourceTable(EvkTables.REF_UNION_CAMPAIGN_DIC_HDP);
        return evk.select(
                col("sk_id"),
                col("ab_test_id"),
                col("product_id"),
                col("product_name"),
                col("product_group_name"),
                col("product_sgroup_name"),
                col("start_dt"),
                col("end_dt"),
                col("wave_launch_dt"),
                col("wave_deactivation_dt"),
                col("unique_segment"),
                col("segment"),
                col("campaign_code"),
                col("campaign_name"),
                col("campaign_name_cf"),
                col("campaign_type"),
                col("rnk"),
                col("campaign_id"),
                col("segment_id"),
                col("wave_id"),
                col("lal_id"),
                col("camp_source"),
                col("message_id"),
                col("template_text"),
                col("channel_id"),
                col("channel_name"),
                col("product_group_id"),
                col("tg"),
                col("template_comment"),
                col("template_title"),
                col("channel_name_crm"),
                col("offer_id"),
                col("team_id"),
                col("channel_type"),
                col("channel_name_rep"),
                col("channel_system"),
                col("camp_source_process"),
                col("auto_launch_flg"),
                col("campaign_trgt_type_name"),
                col("template_text_full"),
                col("template_name"),
                col("params_txt"),
                col("team_name"),
                col("is_manual"),
                col("load_ts")
        ).withColumn("date_build_snp", lit(DATE_BUILD_SNP).cast(StringType));
    }

    public static void main(String[] args) {
        runner().run(EvkCampaignDicSnp.class);
    }
}
