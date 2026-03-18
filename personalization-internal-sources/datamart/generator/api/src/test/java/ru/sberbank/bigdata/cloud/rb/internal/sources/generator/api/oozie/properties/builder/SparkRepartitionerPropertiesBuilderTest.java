package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.properties.builder;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.MemoryParams;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.ActionConf;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SparkRepartitionerPropertiesBuilderTest {

    private final String actionName = "test_schema.test_action_name_repartitioner";

    @Nested
    class getMemoryParams {

        private final ActionConf actionConf = new ActionConf();
        private final SparkRepartitionerPropertiesBuilder propertiesBuilder = new SparkRepartitionerPropertiesBuilder(actionName,
                null, actionConf, null, null);

        @Test
        void fromDatamart() {
            actionConf.setExecutors(66);
            actionConf.setDriverMemory("8G");
            actionConf.setExecutorMemory("10G");
            final MemoryParams memoryParams = propertiesBuilder.getMemoryParams();
            assertEquals(new Integer(66), memoryParams.getNumberOfExecutors());
            assertEquals("10G", memoryParams.getExecutorMemory());
            assertEquals("8G", memoryParams.getDriverMemory());
        }

        @Test
        void defaultMemoryParams() {
            actionConf.setExecutors(null);
            final MemoryParams memoryParams = propertiesBuilder.getMemoryParams();
            assertEquals(new Integer(50), memoryParams.getNumberOfExecutors());
            assertEquals("2G", memoryParams.getExecutorMemory());
            assertEquals("4G", memoryParams.getDriverMemory());
        }
    }
}
