package ru.sberbank.bigdata.cloud.rb.internal.sources.common.service;

import org.apache.spark.sql.SparkSession;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.auto_config.ServiceFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.logging.LoggerTypeId;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.SparkSQLUtil;

import java.util.Arrays;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.SysPropertyTool.safeSystemProperty;

/**
 * Класс осуществляет удаление таблиц переданных в качестве параметра, в том числе и файлы в hdfs
 */
public class TableRemoveRunner {

    private final SparkSession sqlContext;
    private final String tablesToDrop;

    public TableRemoveRunner(SparkSession sqlContext, String tablesToDrop) {
        this.sqlContext = sqlContext;
        this.tablesToDrop = tablesToDrop;
    }

    public void run() {
        Arrays.stream(tablesToDrop.trim().split(";"))
                .map(FullTableName::of)
                .forEach(table -> {
                    SparkSQLUtil.dropTable(sqlContext, table.fullTableName());
                });
    }

    public static void main(String[] args) {
        final String tablesToDrop = safeSystemProperty("tables.drop");
        LoggerTypeId.set("remove_table_runner");
        final String jobName = "remove_table_runner";
        final SparkSession sqlContext = new ServiceFactory(jobName).sqlContext();
        final TableRemoveRunner hiveScriptRunner = new TableRemoveRunner(sqlContext, tablesToDrop);
        hiveScriptRunner.run();
    }
}
