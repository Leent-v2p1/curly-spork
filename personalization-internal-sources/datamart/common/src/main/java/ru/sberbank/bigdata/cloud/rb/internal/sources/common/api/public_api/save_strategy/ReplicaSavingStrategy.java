package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_strategy;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.hive.SchemaGrantsChecker;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.DatamartNaming;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.replica.ReplicaNameResolver;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.save.TableSaveHelper;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.DbName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.SparkSQLUtil;

/**
 * Для обеспечения возможности построения двух  исторических таблиц, использующих одну и ту же таблицу реплики, необходимо
 * сохранять восстановленную за определённый день таблицу реплики в отдельную таблицу с уникальным для нее именем
 */
public class ReplicaSavingStrategy implements HiveSavingStrategy {

    private final ReplicaNameResolver replicaNameResolver;
    private final FullTableName originalReplicaName;
    private final DbName resultName;

    public ReplicaSavingStrategy(ReplicaNameResolver replicaNameResolver, FullTableName originalReplicaName, DbName resultName) {
        this.replicaNameResolver = replicaNameResolver;
        this.originalReplicaName = originalReplicaName;
        this.resultName = resultName;
    }

    @Override
    public void save(SparkSession context, Dataset<Row> datamart, DatamartNaming naming, SchemaGrantsChecker schemaGrantsChecker) {
        schemaGrantsChecker.checkGrantsToSchemaLocation(context, resultName.dbName());

        String recoveredReplicaTableName = replicaNameResolver.resolveRecoveredReplicaFullName(originalReplicaName);
        String recoveredReplicaFullName = resultName.dbName() + "." + recoveredReplicaTableName;

        TableSaveHelper tableSaveHelper = new TableSaveHelper(context);

        log.info("dropping table {}", recoveredReplicaFullName);
        SparkSQLUtil.dropTable(context, recoveredReplicaFullName);

        log.info("creating table {}", recoveredReplicaFullName);
        tableSaveHelper.createTable(datamart.schema(), recoveredReplicaFullName);

        log.info("inserting data into table {}", recoveredReplicaFullName);
        tableSaveHelper.insertOverwriteTable(recoveredReplicaFullName, datamart);
    }
}