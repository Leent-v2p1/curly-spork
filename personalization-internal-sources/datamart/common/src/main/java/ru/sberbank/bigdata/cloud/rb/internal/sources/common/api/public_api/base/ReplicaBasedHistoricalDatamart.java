package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.replica.ReplicaNameResolver;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.TableName;

public abstract class ReplicaBasedHistoricalDatamart extends HistoricalDatamart {

    private boolean recoveryMode;
    private ReplicaNameResolver replicaNameResolver;

    @Override
    public Dataset<Row> sourceTable(TableName tableName) {
        if (recoveryMode) {
            String resolvedTableName = replicaNameResolver.resolveRecoveredReplicaTableName(tableName);
            return super.stageTable(resolvedTableName);
        }
        return super.sourceTable(tableName);
    }

    @Override
    public Dataset<Row> sourceTable(FullTableName fullTableName) {
        if (recoveryMode) {
            String resolvedTableName = replicaNameResolver.resolveRecoveredReplicaFullName(fullTableName);
            return super.stageTable(resolvedTableName);
        }
        return super.sourceTable(fullTableName);
    }

    /*Getters and setters*/

    public boolean isRecoveryMode() {
        return recoveryMode;
    }

    public void setRecoveryMode(boolean recoveryMode) {
        this.recoveryMode = recoveryMode;
    }

    public ReplicaNameResolver getReplicaNameResolver() {
        return replicaNameResolver;
    }

    public void setReplicaNameResolver(ReplicaNameResolver replicaNameResolver) {
        this.replicaNameResolver = replicaNameResolver;
    }
}
