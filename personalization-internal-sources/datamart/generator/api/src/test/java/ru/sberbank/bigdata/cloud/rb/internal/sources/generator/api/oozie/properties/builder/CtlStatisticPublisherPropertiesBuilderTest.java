package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.properties.builder;

import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.MemoryParams;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CtlStatisticPublisherPropertiesBuilderTest {

    private final String actionName = "test_schema.test_action_name_ctl_stats_publisher";
    private final CtlStatisticPublisherPropertiesBuilder propertiesBuilder = new CtlStatisticPublisherPropertiesBuilder(actionName, null, null, null);

    @Test
    void getMemoryParams() {
        final MemoryParams memoryParams = propertiesBuilder.getMemoryParams();
        assertEquals(new Integer(2), memoryParams.getNumberOfExecutors());
        assertEquals("2G", memoryParams.getExecutorMemory());
        assertEquals("2G", memoryParams.getDriverMemory());
    }
}