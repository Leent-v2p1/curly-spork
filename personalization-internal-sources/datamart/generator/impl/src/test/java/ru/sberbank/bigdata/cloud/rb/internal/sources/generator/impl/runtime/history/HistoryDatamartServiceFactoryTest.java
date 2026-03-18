package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.runtime.history;

import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.test_api.DatamartTest;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.MemoryPreset;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.replica_on_date.configuration.ReplicaConf;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HistoryDatamartServiceFactoryTest extends DatamartTest {

    @Test
    void replicaHistoryTable() {
        FullTableName replicaName = FullTableName.of("schema.test_replica_name");
        Map<String, ReplicaConf> testReplicaConfig = new HashMap<>();
        testReplicaConfig.put(replicaName.fullTableName(), new ReplicaConf("test_replica_name", "snapshot_db.table_1", MemoryPreset.NOT_SPECIFIED));
        HistoryDatamartServiceFactory serviceFactory = new HistoryDatamartServiceFactory(testReplicaConfig, replicaName);
        FullTableName actualHistoryTableName = serviceFactory.replicaHistoryTable();
        assertEquals(FullTableName.of("snapshot_db_hist.table_1_hst"), actualHistoryTableName);
    }
}