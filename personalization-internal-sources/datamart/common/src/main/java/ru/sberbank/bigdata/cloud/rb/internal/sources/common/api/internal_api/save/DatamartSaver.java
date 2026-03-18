package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.save;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.QueryExecution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.base.Parametrizer;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.hive.SchemaGrantsChecker;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.DatamartNaming;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_strategy.HiveSavingStrategy;

import java.util.Set;

public class DatamartSaver {

    private static final Logger log = LoggerFactory.getLogger(DatamartSaver.class);

    private final SparkSession context;
    private final DatamartNaming naming;
    private final HiveSavingStrategy savingStrategy;
    private final SchemaGrantsChecker grantsChecker;
    private final Parametrizer parametrizer;

    public DatamartSaver(SparkSession context,
                         DatamartNaming naming,
                         HiveSavingStrategy savingStrategy,
                         SchemaGrantsChecker grantsChecker,
                         Parametrizer parametrizer) {
        this.context = context;
        this.naming = naming;
        this.savingStrategy = savingStrategy;
        this.grantsChecker = grantsChecker;
        this.parametrizer = parametrizer;
    }

    public void save(Dataset<Row> datamart) {
        logExecutionPlan(datamart);
        final Dataset<Row> repartitionedDataframe = parametrizer.applyRepartitionAndCoalesce(datamart, "");
        log.info("Save status: naming: {}, savingStrategy: {}", naming, savingStrategy);
        savingStrategy.save(context, repartitionedDataframe, naming, grantsChecker);
    }

    public void update(Dataset<Row> actual, Dataset<Row> notActual, Set<String> partitionsToAdd) {
        TableSaveHelper tableSaveHelper = new TableSaveHelper(context);
        //save snp
        final FullTableName reserveTableName = FullTableName.of(naming.reserveFullTableName());
        final FullTableName paTableName = FullTableName.of(naming.fullTableName());
        tableSaveHelper.saveReservingSnpTable(actual, reserveTableName, paTableName, partitionsToAdd);
        //save hst
        final FullTableName histPartTableName = FullTableName.of(naming.historyReserveFullTableName());
        final FullTableName histPartPaTableName = FullTableName.of(naming.historyFullTableName());
        tableSaveHelper.saveReservingHistTable(notActual, histPartTableName, histPartPaTableName);
    }

    public HiveSavingStrategy getSavingStrategy() {
        return savingStrategy;
    }

    private void logExecutionPlan(Dataset<Row> datamart) {
        QueryExecution executionPlan = datamart.queryExecution();
        log.info("queryExecution: {}", executionPlan);
    }
}
