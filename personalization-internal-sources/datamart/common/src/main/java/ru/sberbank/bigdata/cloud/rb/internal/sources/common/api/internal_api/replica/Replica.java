package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.replica;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;

import java.util.List;

public abstract class Replica {

    public final ReplicaContext replicaContext;

    public Replica(ReplicaContext replicaContext) {
        this.replicaContext = replicaContext;
    }

    public abstract Dataset<Row> buildReplica();

    public Dataset<Row> sourceReplicaTable(FullTableName fullTableName) {
        return replicaContext.fullNamedTable(fullTableName);
    }

    public List<String> schemaTables(String dbName) {
        return replicaContext.schemaTables(dbName);
    }
}
