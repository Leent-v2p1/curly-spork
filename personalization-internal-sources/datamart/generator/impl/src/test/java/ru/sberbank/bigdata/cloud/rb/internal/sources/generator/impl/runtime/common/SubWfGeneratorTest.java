package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.runtime.common;

import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.WfEntity;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SubWfGeneratorTest {
    @Test
    void mapToWfEntities() {
        String name = "test_name";
        LocalDate start = LocalDate.of(2017, 1, 2);
        LocalDate end = LocalDate.of(2017, 2, 2);

        SubWfGenerator generator = new SubWfGenerator(name, "testAppPath", start, end, true, Environment.PRODUCTION);
        Map<String, Object> stringObjectMap = generator.mapToWfEntities();
        String expectedName = "create-test_name-2017-01";
        assertEquals(expectedName, stringObjectMap.get("firstWf"));
        assertEquals(name, stringObjectMap.get("tableName"));

        @SuppressWarnings("unchecked")
        List<WfEntity> wfs = (List<WfEntity>) stringObjectMap.get("wfs");
        assertEquals(1, wfs.size());
        WfEntity wfEntity = wfs.get(0);
        assertEquals(expectedName, wfEntity.getName());
        assertTrue(wfEntity.getFirstBuild());
    }

    @Test
    void mapToWfEntitiesNotFirstDate() {
        String name = "test_name";
        LocalDate start = LocalDate.of(2017, 1, 1);
        LocalDate end = LocalDate.of(2017, 2, 15);

        SubWfGenerator generator = new SubWfGenerator(name, "testAppPath", start, end, false, Environment.PRODUCTION);
        Map<String, Object> stringObjectMap = generator.mapToWfEntities();
        assertEquals("create-test_name-2017-01", stringObjectMap.get("firstWf"));
        assertEquals(name, stringObjectMap.get("tableName"));

        @SuppressWarnings("unchecked")
        List<WfEntity> wfs = (List<WfEntity>) stringObjectMap.get("wfs");
        assertEquals(1, wfs.size());
        WfEntity wfEntity = wfs.get(0);
        assertEquals("end", wfEntity.getNextName(), "last action should be 'end'");
        assertFalse(wfEntity.getFirstBuild());
    }

    @Test
    void mapToWfEntitiesTestEnv() {
        String name = "test_name";
        LocalDate start = LocalDate.of(2017, 1, 1);
        LocalDate end = LocalDate.of(2018, 2, 15);

        SubWfGenerator generator = new SubWfGenerator(name, "testAppPath", start, end, false, Environment.PRODUCTION_TEST);
        Map<String, Object> stringObjectMap = generator.mapToWfEntities();
        assertEquals("create-test_name-2017-01", stringObjectMap.get("firstWf"));
        assertEquals(name, stringObjectMap.get("tableName"));

        @SuppressWarnings("unchecked")
        List<WfEntity> wfs = (List<WfEntity>) stringObjectMap.get("wfs");
        assertEquals(2, wfs.size());
        WfEntity wfEntity = wfs.get(1);
        assertEquals("end", wfEntity.getNextName(), "last action should be 'end'");
    }
}