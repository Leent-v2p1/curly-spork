package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.config.param_setter;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.util.FileUtils;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.spy;

class CommonSetterTest {
    private static Map<String, String> ctlParams;

    @BeforeAll
    static void setUp() {
        ctlParams = new HashMap<>();
        ctlParams.put("user.name", "some_user");
        ctlParams.put("keytabPath", "some/path");
        ctlParams.put("principal", "user@domain");
    }

    @Test
    void testSystemRun() {
        String allProperty = FileUtils.readAsString(Paths.get("src/test/resources/conf/result/default.conf"));
        CommonSetter setter = spy(new CommonSetter(ctlParams));
        String result = setter.run(allProperty);

        String expected = FileUtils.readAsString(Paths.get("src/test/resources/conf/result/common-system.conf"));
        assertEquals(expected, result);
    }

    @Test
    void testSparkRun() {
        String allProperty = FileUtils.readAsString(Paths.get("src/test/resources/conf/spark.conf"));
        CommonSetter setter = spy(new CommonSetter(ctlParams));
        String result = setter.run(allProperty);

        String expected = FileUtils.readAsString(Paths.get("src/test/resources/conf/result/common-spark.conf"));
        assertEquals(expected, result);
    }
}