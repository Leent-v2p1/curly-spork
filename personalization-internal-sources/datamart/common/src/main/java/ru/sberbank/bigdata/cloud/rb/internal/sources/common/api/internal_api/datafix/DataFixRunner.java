package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.datafix;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.datafix.change_description.ChangeAction;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.history.UpdateExpression;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.DatamartNaming;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.save.TableSaveHelper;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.Datamart;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.history.History;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.hive.PartitionInfo;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.stream.Collectors;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.datafix.DataFixType.SCHEMA_CHANGE;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.SparkSQLUtil.getPartitionInfo;

public class DataFixRunner {

    private static final Logger log = LoggerFactory.getLogger(DataFixRunner.class);

    private final List<DataFix> actions;
    private final SparkSession context;
    private final DatamartNaming naming;
    private final ColumnValidator columnValidator;
    private final BackupService backupService;
    private final Optional<History> history;
    private final TableSaveHelper tableSaveHelper;

    public DataFixRunner(@Nonnull List<DataFix> actions,
                         SparkSession context,
                         DatamartNaming naming,
                         ColumnValidator columnValidator,
                         BackupService backupService,
                         Optional<History> history) {
        this.actions = actions;
        this.context = context;
        this.naming = naming;
        this.columnValidator = columnValidator;
        this.backupService = backupService;
        this.history = history;
        this.tableSaveHelper = new TableSaveHelper(context);
    }

    @SuppressWarnings("unchecked")
    private static <T extends DataFix> List<T> collectChangesByType(Map<DataFixType, List<DataFix>> actionInfo, DataFixType type) {
        return (List<T>) actionInfo.getOrDefault(type, Collections.emptyList());
    }

    public void run(Datamart datamart) {
        Map<DataFixType, List<DataFix>> actionInfo = actions
                .stream()
                .collect(Collectors.groupingBy(DataFix::type));
        List<ChangeAction> schemaChanges = collectChangesByType(actionInfo, SCHEMA_CHANGE);
        log.info("Validating {} schema changes", schemaChanges.size());
        columnValidator.checkSchemaChanges(datamart, schemaChanges);

        backupService.createBackup();
        try {
            //historical datamart
            if (history.isPresent()) {
                changeDatamart(naming.backupFullTableName(), naming.fullTableName(), true);
                changeDatamart(naming.historyBackupFullTableName(), naming.historyFullTableName(), false);
            }
            //regular datamart
            else {
                changeDatamart(naming.backupFullTableName(), naming.fullTableName(), false);
            }
        } catch (DataFixException exception) {
            log.error("Got an exception while applying datafixes: " + exception.getMessage(), exception);
            backupService.restoreTableFromBackup();
        }
    }

    void changeDatamart(String backupTableName, String paTableName, boolean updateHash) {
        Dataset<Row> df = context.table(backupTableName);

        for (DataFix dataFix : actions) {
            try {
                df = dataFix.apply(df);
            } catch (Exception e) {
                throw new DataFixException("Failed while applying datafix: " + dataFix, e);
            }
        }

        if (updateHash && history.isPresent()) {
            df = updateHash(df, history.get());
        }
        rewriteTable(df, FullTableName.of(paTableName));
    }

    Dataset<Row> updateHash(Dataset<Row> updatedDF, History history) {
        final Column[] columns = history.withoutHistory(updatedDF);
        final String columnNames = Arrays.toString(columns);
        log.info("Counting hash for columns: {}", columnNames);
        final Column hash = UpdateExpression.hashViaSha2(updatedDF.select(columns));
        return updatedDF.withColumn("row_hash", hash);
    }

    void rewriteTable(Dataset<Row> datamart, FullTableName tableName) {
        try {
            final PartitionInfo partInfo = getPartitionInfo(context, tableName.fullTableName()).orElse(null);
            tableSaveHelper.savePaTable(datamart, tableName.fullTableName(), partInfo);
        } catch (Exception e) {
            throw new DataFixException("Failed while rewriting table: " + tableName, e);
        }
    }
}
