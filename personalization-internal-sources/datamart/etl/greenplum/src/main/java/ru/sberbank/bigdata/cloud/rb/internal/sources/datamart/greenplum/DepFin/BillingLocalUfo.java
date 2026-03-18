package ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.greenplum.DepFin;

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
import static org.apache.spark.sql.types.DataTypes.StringType;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.AutoConfigDatamartRunner.runner;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId.PROCESSED_LOADING_ID;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.greenplum.GreenplumConstants.*;

@DatamartRef(id = "custom_rb_depfin.v_is0481_billing_local_ufo", name = "Billing Local Ufo", useSystemPropertyToGetId = true)
@PartialReplace(partitioning = "c_orig_doc_data_op", saveRemover = UpdatedPartitionRemover.class)
public class BillingLocalUfo extends Datamart {

    private static final Logger log = LoggerFactory.getLogger(BillingLocalUfo.class);
    private Date endDt;
    private Date startDt;

    @Override
    public HiveSavingStrategy customSavingStrategy(DatamartServiceFactory datamartServiceFactory) {
        return new PartitionedSavingStrategy(PartitionInfo.dynamic().add("c_orig_doc_data_op").create());
    }

    @Override
    public void init(DatamartServiceFactory datamartServiceFactory) {
        final ParametersService parametersService = datamartServiceFactory.parametersService();
        this.endDt = Date.valueOf(parametersService.endCtlParameter().orElse(buildDate()));
        this.startDt = Date.valueOf(parametersService.startCtlParameter().orElse(buildDate().minusMonths(1).withDayOfMonth(1)));
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

        Dataset<Row> billing = spark.read().format("io.pivotal.greenplum.spark.GreenplumRelationProvider")
                .option("dbschema", "s_grnplm_vd_rozn_mpp_daas_bf_vd")
                .option("dbtable", "v_is0481_billing_local_ufo")
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
                : col("c_orig_doc_data_op").gt(startDt).and(col("c_orig_doc_data_op").leq(endDt));

        Dataset<Row> result = billing
                .where(filter)
                .select(
                        col("id_auto"),
                        col("id"),
                        col("class_id"),
                        col("state_id"),
                        col("c_date_exec"),
                        col("c_orig_doc_data_op").cast(StringType).as("c_orig_doc_data_op"),
                        col("c_filial"),
                        col("c_cr_req_part"),
                        col("c_cr_req_term_id"),
                        col("c_code_visa"),
                        col("c_term_type_code"),
                        col("c_pay_meth_code"),
                        col("c_pay_meth_id"),
                        col("c_analit_id"),
                        col("c_num"),
                        col("c_scname"),
                        col("c_kod"),
                        col("c_req_2_2"),
                        col("c_comment"),
                        col("c_name"),
                        col("c_svod"),
                        col("c_inn"),
                        col("c_kpp"),
                        col("c_rc_req_part"),
                        col("c_rc_req_serv_ref"),
                        col("c_reestr"),
                        col("c_acc"),
                        col("c_bic"),
                        col("c_bank_name"),
                        col("c_doc_name"),
                        col("c_scan"),
                        col("c_ks"),
                        col("c_orig_doc_summa"),
                        col("c_fil_komm"),
                        col("c_bank_komm"),
                        col("c_agent_komm"),
                        col("c_agent_komm_rec"),
                        col("c_agent_komm_payer"),
                        col("c_cn_komm_rec"),
                        col("c_cn_komm_payer"),
                        col("c_bank_komm_payer"),
                        col("c_komm_rec"),
                        col("c_sum_dt"),
                        col("c_sum_kt"),
                        col("c_sc"),
                        col("c_kolich"),
                        col("c_user_okr"),
                        col("c_state_id"),
                        col("c_bud_req_kbk_str"),
                        col("c_bud_req_okato_str"),
                        col("c_masspay_req_num_card"),
                        col("c_masspay_req_fio"),
                        col("c_ident_data"),
                        col("c_filial_term"),
                        col("c_zone_code"),
                        col("c_zone_name"),
                        col("c_part_depcode"),
                        col("cnt_"),
                        col("tb_id"),
                        col("osb_id"),
                        col("vsp_id"),
                        col("c_tb"),
                        col("isdog"),
                        col("scan"),
                        col("c_cr_req_term_id_cl"),
                        col("c_code_visa_cl"),
                        col("c_pachka"),
                        col("c_key_props"),
                        col("chanel"),
                        col("us_id"),
                        col("descr"),
                        col("last_4_dig_card"),
                        col("population_time"),
                        col("dttm_orig"),
                        col("suip"),
                        col("tid"),
                        col("fee_after_pay"),
                        col("channel_good"),
                        col("gosb_id"),
                        col("payment_type"),
                        col("from_sv"),
                        col("outsyst_id"),
                        col("epk_id"),
                        col("tm_options")
                );

        return result;
    }

    public static void main(String[] args) {
        runner().run(BillingLocalUfo.class);
    }
}
