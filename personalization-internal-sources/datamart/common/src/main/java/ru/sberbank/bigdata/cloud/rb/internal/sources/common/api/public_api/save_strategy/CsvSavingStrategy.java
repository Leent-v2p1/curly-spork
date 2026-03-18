package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_strategy;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.hive.SchemaGrantsChecker;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.DatamartNaming;

public class CsvSavingStrategy implements HiveSavingStrategy {

    private final String path;

    public CsvSavingStrategy(String path) {
        this.path = path;
    }

    @Override
    public void save(SparkSession context, Dataset<Row> datamart, DatamartNaming naming, SchemaGrantsChecker schemaGrantsChecker) {
        schemaGrantsChecker.checkGrantsToSchemaLocation(context, naming.resultSchema());
        log.info("Saving df to in csv format to location {}", path);
        datamart.write()
                .format("com.databricks.spark.csv")
                .option("delimiter", "\t")
                .option("inferSchema", "true")
                .option("header", "false")
                .option("nullValue", "NULL")
                .mode(SaveMode.Overwrite)
                .save(path);
    }
}
