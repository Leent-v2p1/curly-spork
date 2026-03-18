package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.config.param_setter;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.util.FileUtils;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.spy;

class ExecutionSetterTest {
    private static String allProperty;
    private static ExecutionSetter setter;
    private static String expected;

    @BeforeAll
    static void setUp() {
        allProperty = FileUtils.readAsString(Paths.get("src/test/resources/conf/result/common-spark.conf"));
        Map<String, String> newProperty = new HashMap<>();
        newProperty.put("datamart.test_table.executorMemory", "2G");
        newProperty.put("datamart.all.executors", "3");
        expected = FileUtils.readAsString(Paths.get("src/test/resources/conf/result/execution.conf"));
        setter = spy(new ExecutionSetter(newProperty, "test_table"));
    }

    @Test
    void testRun() {
        String result = setter.run(allProperty);
        assertEquals(expected, result);
    }

//    @Nested
//    class ParamResolver {
//        @Test
//        void testAllParam() {
//            assertEquals("executors", setter.resolveParam("datamart.all.executors"));
//        }
//
//        @Test
//        void testTableParam() {
//            assertEquals("executors", setter.resolveParam("datamart.test_table.executors"));
//        }
//
//        @Test
//        void testWrongParam() {
//            assertEquals(StringUtils.EMPTY, setter.resolveParam("datamart.all.aaabbb"));
//        }
//
//        @Test
//        void testOtherParam() {
//            assertEquals(StringUtils.EMPTY, setter.resolveParam("test_table.executors"));
//        }
//    }
}