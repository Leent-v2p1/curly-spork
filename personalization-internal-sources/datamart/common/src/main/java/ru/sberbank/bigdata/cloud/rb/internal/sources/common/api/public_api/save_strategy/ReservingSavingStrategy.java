package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_strategy;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.hive.SchemaGrantsChecker;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.DatamartNaming;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.save.TableSaveHelper;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.SparkSQLUtil;

public class ReservingSavingStrategy implements HiveSavingStrategy {
    @Override
    public void save(SparkSession context, Dataset<Row> datamart, DatamartNaming naming, SchemaGrantsChecker schemaGrantsChecker) {
        schemaGrantsChecker.checkGrantsToSchemaLocation(context, naming.resultSchema());
        String reserveTable = naming.reserveFullTableName();
        String paTable = naming.fullTableName();
        TableSaveHelper tableSaveHelper = new TableSaveHelper(context);
        SparkSQLUtil.dropTable(context, reserveTable);
        log.info("creating reserve table {}", reserveTable);
        tableSaveHelper.createReserveTable(datamart.schema(), reserveTable, paTable);
        log.info("inserting data into reserve table {}", reserveTable);
        tableSaveHelper.insertOverwriteTable(reserveTable, datamart);
    }
}
