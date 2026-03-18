package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.replica_on_date.configuration;

import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.base.WorkflowType;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.YamlPropertiesParser;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.NameAdditions.PROPERTIES_POSTFIX;

class ReplicaActionDependencyTest {

    @Test
    void read() {
        ReplicaActionDependency replicaActionDependency = new ReplicaActionDependency(new YamlPropertiesParser().parse("/repilca-config-parser/test-datamart-properties.yaml"));
        Map<String, ReplicaConf> actualReplicaConfigs = replicaActionDependency.resolve();
        assertThat(actualReplicaConfigs.entrySet(), hasSize(4));
        ReplicaConf replicaConf1 = actualReplicaConfigs.get("custom_rb_card_stg.f_i_hst");
        assertEquals("internal_way4_ows.f_i", replicaConf1.getSnapshot());
        assertEquals(new Integer(33), replicaConf1.getExecutors());
        assertEquals("3G", replicaConf1.getExecutorMemory());
        assertEquals("4G", replicaConf1.getDriverMemory());
        assertEquals(new Integer(1600), replicaConf1.getDriverMemoryOverhead());
        assertEquals(new Integer(900), replicaConf1.getExecutorMemoryOverhead());

        ReplicaConf replicaPropertiesConf1 = actualReplicaConfigs.get("custom_rb_card_stg.f_i_hst" + PROPERTIES_POSTFIX);
        assertEquals(WorkflowType.PROPERTIES.getKey(), replicaPropertiesConf1.getType());

        ReplicaConf replicaConf2 = actualReplicaConfigs.get("custom_rb_card_stg.doc_hst");
        assertEquals("internal_way4_ows.doc", replicaConf2.getSnapshot());
        assertEquals("4G", replicaConf2.getDriverMemory());
        assertEquals(new Integer(40), replicaConf2.getExecutors());
        assertEquals("4G", replicaConf2.getExecutorMemory());
        assertEquals(new Integer(1600), replicaConf2.getDriverMemoryOverhead());
        assertEquals(new Integer(900), replicaConf2.getExecutorMemoryOverhead());

        ReplicaConf replicaPropertiesConf2 = actualReplicaConfigs.get("custom_rb_card_stg.doc_hst" + PROPERTIES_POSTFIX);
        assertEquals(WorkflowType.PROPERTIES.getKey(), replicaPropertiesConf2.getType());
    }
}