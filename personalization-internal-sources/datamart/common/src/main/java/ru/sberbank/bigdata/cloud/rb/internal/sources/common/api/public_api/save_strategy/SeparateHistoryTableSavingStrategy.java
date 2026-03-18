package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_strategy;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.hive.SchemaGrantsChecker;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.DatamartNaming;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.save.TableSaveHelper;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.hive.PartitionInfo;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.SparkSQLUtil;

import java.util.Collections;

import static org.apache.spark.sql.types.DataTypes.TimestampType;

public class SeparateHistoryTableSavingStrategy implements HiveSavingStrategy {

    public static final String ACTUAL_PARTITION_COLUMN = "row_actual_from_month";
    public static final String HISTORY_PARTITION_COLUMN = "row_actual_to_month";
    public static final PartitionInfo HISTORY_PARTITIONING = PartitionInfo.dynamic()
            .add(HISTORY_PARTITION_COLUMN)
            .create();
    public static final PartitionInfo SNAPSHOT_PARTITIONING = PartitionInfo.dynamic()
            .add(ACTUAL_PARTITION_COLUMN)
            .create();

    /**
     * Создаем не только снэпшот витрины, но и заготовку для исторической части.
     * Но она остается пустой до первого построения истории
     */
    @Override
    public void save(SparkSession context, Dataset<Row> datamart, DatamartNaming naming, SchemaGrantsChecker schemaGrantsChecker) {
        TableSaveHelper tableSaveHelper = new TableSaveHelper(context);
        schemaGrantsChecker.checkGrantsToSchemaLocation(context, naming.resultSchema());
        // create snp
        FullTableName fullTableName = FullTableName.of(naming.reserveFullTableName());
        FullTableName paTableName = FullTableName.of(naming.fullTableName());
        tableSaveHelper.saveReservingSnpTable(datamart, fullTableName, paTableName, Collections.emptySet());
        // create history
        SparkSQLUtil.dropTable(context, naming.historyReserveFullTableName());
        final StructType historyPartSchema = datamart
                .drop(ACTUAL_PARTITION_COLUMN)
                .schema()
                .add("row_actual_to_dt", TimestampType)
                .add("ctl_validto", TimestampType);
        tableSaveHelper.createPartitionedReserveTable(historyPartSchema, naming.historyReserveFullTableName(), naming.historyFullTableName(), HISTORY_PARTITIONING);
    }
}
