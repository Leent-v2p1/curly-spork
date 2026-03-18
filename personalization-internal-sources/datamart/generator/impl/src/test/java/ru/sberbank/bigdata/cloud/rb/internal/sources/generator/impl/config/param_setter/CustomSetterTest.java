package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.config.param_setter;

import org.apache.commons.lang.StringUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.util.FileUtils;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.spy;

class CustomSetterTest {
    private static String allProperty;
    private static CustomSetter setter;
    private static String expected;

    @BeforeAll
    static void setUp() {
        allProperty = FileUtils.readAsString(Paths.get("src/test/resources/conf/result/common-system.conf"));
        Map<String, String> newProperty = new HashMap<>();
        newProperty.put("datamart.test_table.spark.yarn.executor.memoryOverhead", "10");
        newProperty.put("datamart.all.savetemp.another_table", "false");
        newProperty.put("datamart.test_table.executorMemory", "true");
        expected = FileUtils.readAsString(Paths.get("src/test/resources/conf/result/custom.conf"));
        setter = spy(new CustomSetter(newProperty, "test_table"));
    }

    @Test
    void testRun() {
        String result = setter.run(allProperty);
        assertEquals(expected, result);
    }

    @Nested
    class ParamResolverTest {
        @Test
        void allParam() {
            assertEquals("yarn.executor", setter.resolveParam("datamart.all.spark.yarn.executor"));
        }

        @Test
        void tableParam() {
            assertEquals("aabbcc", setter.resolveParam("datamart.test_table.spark.aabbcc"));
        }

        @Test
        void customParam() {
            assertEquals("savetemp.abc", setter.resolveParam("datamart.all.savetemp.abc"));
        }

        @Test
        void skipExecutionParam() {
            assertEquals(StringUtils.EMPTY, setter.resolveParam("datamart.test_table.executors"));
        }

        @Test
        void skipOtherParam() {
            assertEquals(StringUtils.EMPTY, setter.resolveParam("yarn.configuration"));
        }
    }
}