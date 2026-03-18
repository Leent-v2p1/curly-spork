package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.replica_on_date.configuration;

import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.NameAdditions;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.MemoryPreset;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ReplicaExtractorTest {

    @Test
    void getDatamartReplicaTables() {
        Map<String, ReplicaConf> stringReplicaConfMap = new HashMap<>();
        stringReplicaConfMap.put("abc_stg.a1", new ReplicaConf("a", null, MemoryPreset.LOW));
        stringReplicaConfMap.put("abc_stg.a1" + NameAdditions.PROPERTIES_POSTFIX, new ReplicaConf("a", null, MemoryPreset.LOW));
        stringReplicaConfMap.put("abc_stg.a2", new ReplicaConf("b", null, MemoryPreset.LOW));
        stringReplicaConfMap.put("abc_stg.a2" + NameAdditions.PROPERTIES_POSTFIX, new ReplicaConf("b", null, MemoryPreset.LOW));
        stringReplicaConfMap.put("abc3_stg.a2", new ReplicaConf("c", null, MemoryPreset.LOW));
        ReplicaExtractor replicaExtractor = new ReplicaExtractor(stringReplicaConfMap);

        Map<String, ReplicaConf> datamartReplicaTables = replicaExtractor.getReplicaTableConfigurations(Arrays.asList("a1", "a2"), "abc");
        assertThat(datamartReplicaTables.entrySet(), hasSize(4));
        assertEquals("a", datamartReplicaTables.get("abc_stg.a1").getName());
        assertEquals("a", datamartReplicaTables.get("abc_stg.a1" + NameAdditions.PROPERTIES_POSTFIX).getName());
        assertEquals("b", datamartReplicaTables.get("abc_stg.a2").getName());
        assertEquals("b", datamartReplicaTables.get("abc_stg.a2" + NameAdditions.PROPERTIES_POSTFIX).getName());
    }
}