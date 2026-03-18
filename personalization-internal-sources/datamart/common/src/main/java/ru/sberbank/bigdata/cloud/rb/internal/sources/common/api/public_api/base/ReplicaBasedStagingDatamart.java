package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.replica.ReplicaNameResolver;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.TableName;

public abstract class ReplicaBasedStagingDatamart extends StagingDatamart {

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
