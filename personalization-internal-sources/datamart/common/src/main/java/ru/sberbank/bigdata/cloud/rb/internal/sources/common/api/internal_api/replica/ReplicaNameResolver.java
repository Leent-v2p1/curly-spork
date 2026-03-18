package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.replica;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.TableName;

public class ReplicaNameResolver {
    private static Logger log = LoggerFactory.getLogger(ReplicaNameResolver.class);

    private final FullTableName datamart;
    private final String defaultSourceSchema;

    public ReplicaNameResolver(FullTableName datamart, String defaultSourceSchema) {
        this.datamart = datamart;
        this.defaultSourceSchema = defaultSourceSchema;
    }

    /**
     * @return имя таблицы восстановленной реплики в формате: REPLICA_NAME_for_DATAMART_NAME_from_REPLICA_SCHEMA
     */
    public String resolveRecoveredReplicaFullName(FullTableName replica) {
        String resolvedTableName = resolveTableName(replica, replica.dbName());
        log.info("resolve name for datamart {} and replica {}: {}", datamart, replica, resolvedTableName);
        return resolvedTableName;
    }

    /**
     * @return имя таблицы восстановленной реплики в формате: REPLICA_NAME_for_DATAMART_NAME_from_REPLICA_SCHEMA
     */
    public String resolveRecoveredReplicaTableName(TableName replicaTableName) {
        String resolvedTableName = resolveTableName(replicaTableName, defaultSourceSchema);
        log.info("resolve table name for datamart {} and replica table {} : {}", datamart, replicaTableName, resolvedTableName);
        return resolvedTableName;
    }

    private String resolveTableName(TableName replicaTableName, String sourceSchema) {
        return replicaTableName.tableName() + "_for_" + datamart.tableName() + "_from_" + sourceSchema;
    }
}
