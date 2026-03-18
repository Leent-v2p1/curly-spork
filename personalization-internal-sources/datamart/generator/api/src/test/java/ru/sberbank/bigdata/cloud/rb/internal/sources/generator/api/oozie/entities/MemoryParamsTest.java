package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities;

import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.DatamartProperties;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.YamlPropertiesParser;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.ActionConf;

import java.io.File;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.builders.SparkJobParameters.DRIVER_OVERHEAD;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.builders.SparkJobParameters.SHUFFLE_PARTITIONS;

class MemoryParamsTest {

    @Test
    void testProperties() {
        final String expectedDriverMemory = "8G";
        final String expectedExecutorMemory = "4G";
        final Integer expectedExecutorCoreNum = 5;
        final Integer expectedNumExecutors = 49;
        final Integer expectedExecutorMemoryOverhead = 901;
        //given:

        Map<String, Object> properties = new YamlPropertiesParser().parse(new File("src/test/resources/yaml_config/memory_params.yaml"));
        DatamartProperties propertiesService = new DatamartProperties(FullTableName.of("source_name.table_name"), properties);
        //when:
        MemoryParams actual = MemoryParams.memoryParamsBasedOnPropertiesService(propertiesService, MemoryParams.defaultMemoryParams());
        //then:
        assertEquals(expectedExecutorMemory, actual.getExecutorMemory());
        assertEquals(expectedDriverMemory, actual.getDriverMemory());
        assertEquals(expectedNumExecutors, actual.getNumberOfExecutors());
        assertEquals(expectedExecutorCoreNum, actual.getExecutorCoreNum());
        assertEquals(DRIVER_OVERHEAD, actual.getDriverMemoryOverhead());
        assertEquals(expectedExecutorMemoryOverhead, actual.getExecutorMemoryOverhead());
        assertEquals(SHUFFLE_PARTITIONS, actual.getSparkShufflePartitions());
    }

    @Test
    void testDefaultConf() {
        final String expectedDriverMemory = "4G";
        final String expectedExecutorMemory = "2G";
        final Integer expectedExecutorCoreNum = 1;
        final Integer expectedNumExecutors = 50;
        final Integer expectedExecutorMemoryOverhead = 900;
        //given:
        ActionConf actionConf = new ActionConf();
        //when:
        MemoryParams actual = MemoryParams.memoryParamsBasedOnActionConf(actionConf, MemoryParams.defaultMemoryParams());
        //then:
        assertEquals(expectedExecutorMemory, actual.getExecutorMemory());
        assertEquals(expectedDriverMemory, actual.getDriverMemory());
        assertEquals(expectedNumExecutors, actual.getNumberOfExecutors());
        assertEquals(expectedExecutorCoreNum, actual.getExecutorCoreNum());
        assertEquals(DRIVER_OVERHEAD, actual.getDriverMemoryOverhead());
        assertEquals(expectedExecutorMemoryOverhead, actual.getExecutorMemoryOverhead());
        assertEquals(SHUFFLE_PARTITIONS, actual.getSparkShufflePartitions());
    }

    @Test
    void testPresetConf() {
        final String expectedDriverMemory = "4G";
        final String expectedExecutorMemory = "4G";
        final Integer expectedExecutorCoreNum = 1;
        final Integer expectedNumExecutors = 40;
        final Integer expectedExecutorMemoryOverhead = 900;
        //given:
        ActionConf actionConf = new ActionConf();
        actionConf.setMemoryPreset("MID");
        //when:
        MemoryParams actual = MemoryParams.memoryParamsForRepartitioner(actionConf, MemoryParams.defaultMemoryParams());
        //then:
        assertEquals(expectedExecutorMemory, actual.getExecutorMemory());
        assertEquals(expectedDriverMemory, actual.getDriverMemory());
        assertEquals(expectedNumExecutors, actual.getNumberOfExecutors());
        assertEquals(expectedExecutorCoreNum, actual.getExecutorCoreNum());
        assertEquals(DRIVER_OVERHEAD, actual.getDriverMemoryOverhead());
        assertEquals(expectedExecutorMemoryOverhead, actual.getExecutorMemoryOverhead());
        assertEquals(SHUFFLE_PARTITIONS, actual.getSparkShufflePartitions());
    }
}