package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.runtime.history.debug;

import org.apache.spark.sql.*;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.auto_config.ServiceFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.SparkSQLUtil;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

public class LoanAgreementHistoryDebugGenerator {
    private final SparkSession sc;

    public LoanAgreementHistoryDebugGenerator(SparkSession sc) {
        this.sc = sc;
    }

    protected void run() {
        final LocalDateTime date = LocalDate.of(2018, Month.JUNE, 18).atStartOfDay();

        final Dataset<Row> prCred = sc.table("prx_hdp2_psdp_ekp_reports_internal_ekp_reports_ibs.z_pr_cred");
        final Column mod10 = col("id").mod(10);
        final Dataset<Row> newRows = prCred
                .where(mod10.isin(2, 3, 4, 5))
                .drop("ctl_action")
                .drop("ctl_validfrom")
                .drop("ctl_validto")
                .withColumn("ctl_action", lit("I"))
                .withColumn("ctl_validfrom", lit(Timestamp.valueOf(date)));

        final Dataset<Row> updatedRows = prCred
                .where(mod10.isin(6, 7, 8, 9, 0))
                .drop("ctl_action")
                .drop("ctl_validfrom")
                .drop("ctl_validto");
        final Dataset<Row> updatedRowsSnp = updatedRows
                .withColumn("ctl_action", lit("U"))
                .withColumn("ctl_validfrom", lit(Timestamp.valueOf(date)))
                .withColumn("c_client", lit(new BigDecimal("11111111111111111111111111.000000000000")));
        final Dataset<Row> updatedRowsHist = updatedRows
                .withColumn("ctl_action", lit("I"))
                .withColumn("ctl_validfrom", lit(Timestamp.valueOf(date.minusDays(1))))
                .withColumn("ctl_validto", lit(Timestamp.valueOf(date.minusDays(1))));

        final Dataset<Row> deletedRows = prCred.where(mod10.isin(1))
                .drop("ctl_action")
                .drop("ctl_validfrom")
                .drop("ctl_validto")
                .withColumn("ctl_action", lit("D"))
                .withColumn("ctl_validfrom", lit(Timestamp.valueOf(date.minusDays(1))))
                .withColumn("ctl_validto", lit(Timestamp.valueOf(date.minusDays(1))));

        SparkSQLUtil.unionAll(newRows, updatedRowsSnp)
                .write()
                .mode(SaveMode.Overwrite)
                .saveAsTable("custom_rb_loan_dev_stg.z_pr_cred");
        SparkSQLUtil.unionAll(updatedRowsHist, deletedRows)
                .write()
                .mode(SaveMode.Overwrite)
                .saveAsTable("custom_rb_loan_dev_stg.z_pr_cred_hist_0_20180510000000");
    }

    public static void main(String[] args) {
        final SparkSession hiveContext = new ServiceFactory("gen").sqlContext();
        new LoanAgreementHistoryDebugGenerator(hiveContext).run();
    }
}
