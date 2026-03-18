package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.replica;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.hive.SchemaGrantsChecker;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_strategy.HiveSavingStrategy;

import java.util.List;

import static java.util.Arrays.asList;

public class ReplicaContext {
    private static final Logger log = LoggerFactory.getLogger(ReplicaContext.class);

    private SparkSession context;
    private SchemaGrantsChecker grantsChecker;
    private HiveSavingStrategy savingStrategy;

    public ReplicaContext(SparkSession context, SchemaGrantsChecker grantsChecker, HiveSavingStrategy savingStrategy) {
        this.context = context;
        this.grantsChecker = grantsChecker;
        this.savingStrategy = savingStrategy;
    }

    public Dataset<Row> fullNamedTable(FullTableName fullName) {
        log.info("fullNamedTable = {}", fullName);
        grantsChecker.checkAccess(fullName);
        return context.table(fullName.fullTableName());
    }

    public List<String> schemaTables(String schema) {
        return asList(context.sqlContext().tableNames(schema));
    }

    public void save(Dataset<Row> replica) {
        savingStrategy.save(context, replica, null, grantsChecker);
    }
}
