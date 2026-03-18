package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_strategy;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.hive.SchemaGrantsChecker;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.DatamartNaming;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.save.TableSaveHelper;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.SparkSQLUtil;

public class OverwriteSavingStartegy implements HiveSavingStrategy {

    @Override
    public void save(SparkSession context, Dataset<Row> datamart, DatamartNaming naming, SchemaGrantsChecker schemaGrantsChecker) {
        schemaGrantsChecker.checkGrantsToSchemaLocation(context, naming.resultSchema());
        String tableName = naming.fullTableName();
        log.info("overwrite table {}", tableName);
        TableSaveHelper tableSaveHelper = new TableSaveHelper(context);
        log.info("dropping table {}", tableName);
        SparkSQLUtil.dropTable(context, tableName);
        log.info("creating table {}", tableName);
        tableSaveHelper.createTable(datamart.schema(), tableName);
        log.info("inserting data into table {}", tableName);
        tableSaveHelper.insertOverwriteTable(tableName, datamart);
    }
}
