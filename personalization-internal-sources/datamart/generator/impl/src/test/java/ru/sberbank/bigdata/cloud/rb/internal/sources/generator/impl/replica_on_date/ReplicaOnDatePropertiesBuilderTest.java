package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.replica_on_date;

import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.base.WorkflowType;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.DatamartProperties;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.YamlPropertiesParser;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.MemoryPreset;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.replica_on_date.configuration.ReplicaConf;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ReplicaOnDatePropertiesBuilderTest {

    private static final FullTableName DATAMART_NAME = FullTableName.of("test", "test1");
    private static final String ACTION_NAME = "custom_rb_test.test";

    @Test
    void testCreateReplicaProperties() {
        ReplicaConf conf = createConf();
        Map<String, Object> yamlConf = new YamlPropertiesParser().parse("/yaml_config/test-properties.yaml");
        DatamartProperties properties = new DatamartProperties(FullTableName.of(ACTION_NAME), yamlConf);

        ReplicaOnDatePropertiesBuilder builder = new ReplicaOnDatePropertiesBuilder(
                "custom_rb_test.test", conf, Environment.PRODUCTION, DATAMART_NAME);
        String systemProperties = builder.buildSystemProperties();

        assertTrue(systemProperties.contains("spark.result.schema=custom_rb_test"));
        assertTrue(systemProperties.contains("spark.result.table=test"));
        assertTrue(systemProperties.contains("spark.datamart.parent.table.name=test.test1"));
        assertTrue(systemProperties.contains("spark.datamart.history.table.name=custom_rb_test.test"));
    }

    private ReplicaConf createConf() {
        ReplicaConf conf = new ReplicaConf("replicaTable1", "", MemoryPreset.HIGH);
        conf.setName("spark-app-name");
        conf.setType(WorkflowType.DATAMART.getKey());
        conf.setSourceSchema("custom_rb_test");
        conf.setClassName("ru.my.Class");
        return conf;
    }
}