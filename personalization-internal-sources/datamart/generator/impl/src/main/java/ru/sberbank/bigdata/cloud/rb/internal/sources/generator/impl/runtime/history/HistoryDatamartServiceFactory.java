package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.runtime.history;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.auto_config.ServiceFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.replica.ReplicaContext;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.replica.ReplicaNameResolver;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_strategy.ReplicaSavingStrategy;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.replica_on_date.configuration.ReplicaConf;

import java.util.Map;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.NameAdditions.HIST_POSTFIX;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.NameAdditions.HST_POSTFIX;

class HistoryDatamartServiceFactory extends ServiceFactory {

    private final Map<String, ReplicaConf> replicas;
    private final FullTableName replicaName;

    HistoryDatamartServiceFactory(Map<String, ReplicaConf> replicas, FullTableName replicaName) {
        super(replicaName.fullTableName());
        this.replicas = replicas;
        this.replicaName = replicaName;
    }

    FullTableName replicaSnapshotTable() {
        return FullTableName.of(replicas.get(replicaName.fullTableName()).getSnapshot());
    }

    FullTableName replicaHistoryTable() {
        FullTableName snapshot = replicaSnapshotTable();
        String historySchema = snapshot.dbName() + HIST_POSTFIX;
        String historyTable = snapshot.tableName() + HST_POSTFIX;
        return FullTableName.of(historySchema, historyTable);
    }

    ReplicaContext replicaOnDateContext(FullTableName parentDatamartName) {
        ReplicaNameResolver resolver = new ReplicaNameResolver(parentDatamartName, replicaSnapshotTable().dbName());
        ReplicaSavingStrategy replicaSavingStrategy = new ReplicaSavingStrategy(resolver, replicaSnapshotTable(), replicaName);
        return new ReplicaContext(sqlContext(), schemaGrantsChecker(), replicaSavingStrategy);
    }
}
