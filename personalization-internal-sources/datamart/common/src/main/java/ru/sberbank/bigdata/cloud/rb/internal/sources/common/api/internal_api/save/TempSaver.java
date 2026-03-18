package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.save;

import com.google.common.base.Preconditions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.QueryExecution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.base.Parametrizer;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.hive.HiveDatamartContext;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.DatamartNaming;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.SparkSQLUtil;

public class TempSaver {
    private static final Logger log = LoggerFactory.getLogger(TempSaver.class);

    private final SparkSession context;
    private final DatamartNaming naming;
    private final Parametrizer parametrizer;

    public TempSaver(SparkSession context,
                     DatamartNaming naming,
                     Parametrizer parametrizer) {
        this.context = context;
        this.naming = naming;
        this.parametrizer = parametrizer;
    }

    public Dataset<Row> saveTemp(Dataset<Row> dataFrame, String tempTableName) {
        if (!parametrizer.checkCondition("savetemp." + tempTableName, true)) {
            log.info("Skip saving temporary table {} by custom ctl config", tempTableName);
            return dataFrame;
        }

        final Dataset<Row> repartitionedDf = parametrizer.applyRepartitionAndCoalesce(dataFrame, tempTableName);
        Preconditions.checkArgument(!tempTableName.endsWith(HiveDatamartContext.TEMP_TABLE_POSTFIX),
                "Your favorite name for saving temp table contains " + HiveDatamartContext.TEMP_TABLE_POSTFIX +
                        ". Will fail with unexpected result. Choose another name.");
        final String tempFullTableNameWithPostfix = naming.stageSaveTemp(tempTableName);
        return saveTempCheckedName(repartitionedDf, tempFullTableNameWithPostfix);
    }

    /**
     * Saves temp table without name modification
     */
    public void saveTempStage(Dataset<Row> dataFrame, String tempTableName) {
        final String tempFullTableName = naming.stage(tempTableName);
        saveTempCheckedName(dataFrame, tempFullTableName);
    }

    public Dataset<Row> saveSelfAsTemp(Dataset<Row> dataFrame) {
        final String fullName = naming.stageSaveTemp(HiveDatamartContext.TEMP_TABLE_POSTFIX);
        return saveTempCheckedName(dataFrame, fullName);
    }

    private Dataset<Row> saveTempCheckedName(Dataset<Row> dataFrame, String fullName) {
        log.info("Overwriting {}", fullName);
        logExecutionPlan(dataFrame);
        final TableSaveHelper tableSaveHelper = new TableSaveHelper(context);
        SparkSQLUtil.dropTable(context, fullName);
        tableSaveHelper.createTable(dataFrame.schema(), fullName);
        tableSaveHelper.insertOverwriteTable(fullName, dataFrame);
        return context.table(fullName);
    }

    private void logExecutionPlan(Dataset<Row> datamart) {
        QueryExecution executionPlan = datamart.queryExecution();
        log.info("queryExecution: {}", executionPlan);
    }
}
