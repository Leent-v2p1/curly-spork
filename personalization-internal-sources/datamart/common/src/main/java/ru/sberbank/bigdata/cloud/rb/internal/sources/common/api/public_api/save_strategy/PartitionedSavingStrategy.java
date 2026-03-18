package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_strategy;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.hive.SchemaGrantsChecker;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.DatamartNaming;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.save.TableSaveHelper;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.hive.PartitionInfo;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.SysPropertyTool;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.SparkSQLUtil;

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.FieldConstants.*;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.PropertyConstants.ENABLED_FLAG;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.PropertyConstants.SAVE_SALTING_PROPERTY;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.PropertyConstants.SPARK_OPTION_PREFIX;

public class PartitionedSavingStrategy implements HiveSavingStrategy {

    protected static final String PRESAVE_POSTFIX = "result";
    private final PartitionInfo partitionInfo;

    public PartitionedSavingStrategy(PartitionInfo partitionInfo) {
        this.partitionInfo = partitionInfo;
    }

    public static PartitionedSavingStrategy withMonthPart() {
        return new PartitionedSavingStrategy(PartitionInfo.dynamic().add(MONTH_PART).create());
    }

    public static PartitionedSavingStrategy withDayPart() {
        return new PartitionedSavingStrategy(PartitionInfo.dynamic().add(DAY_PART).create());
    }

    public static PartitionedSavingStrategy withCtlLoadingPartPart() {
        return new PartitionedSavingStrategy(PartitionInfo.dynamic().add(CTL_LOADING_PART, IntegerType).create());
    }

    public static PartitionedSavingStrategy withEribSchemaPart() {
        return new PartitionedSavingStrategy(PartitionInfo.dynamic().add(ERIB_SCHEMA).create());
    }

    public PartitionInfo getPartitionInfo() {
        return partitionInfo;
    }

    @Override
    public void save(SparkSession context, Dataset<Row> datamart, DatamartNaming naming, SchemaGrantsChecker schemaGrantsChecker) {
        schemaGrantsChecker.checkGrantsToSchemaLocation(context, naming.resultSchema());
        final TableSaveHelper tableSaveHelper = new TableSaveHelper(context);
        final FullTableName reserveTableName = FullTableName.of(naming.reserveFullTableName());
        final FullTableName paTableName = FullTableName.of(naming.fullTableName());

        if (ENABLED_FLAG.equals(SysPropertyTool.getSystemProperty(SPARK_OPTION_PREFIX + SAVE_SALTING_PROPERTY))) {
            datamart = saveTempTable(naming.stageSaveTemp(PRESAVE_POSTFIX), datamart, context, tableSaveHelper);
            tableSaveHelper.saveReservingPartitionedTable(datamart, reserveTableName, paTableName, partitionInfo, false);
        } else {
            tableSaveHelper.saveReservingPartitionedTable(datamart, reserveTableName, paTableName, partitionInfo, false);
        }
    }

    private Dataset<Row> saveTempTable(String fullName, Dataset<Row> dataFrame, SparkSession context, TableSaveHelper helper) {
        SparkSQLUtil.dropTable(context, fullName);
        helper.createTable(dataFrame.schema(), fullName);
        helper.insertOverwriteTable(fullName, dataFrame);
        return context.table(fullName);
    }
}
