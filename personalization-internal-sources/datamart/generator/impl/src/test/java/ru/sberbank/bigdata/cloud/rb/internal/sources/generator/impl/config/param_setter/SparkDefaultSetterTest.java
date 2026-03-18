package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.config.param_setter;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.util.FileUtils;

import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

class SparkDefaultSetterTest {
    private static String allProperty;
    private static String newProperty;
    private static String expected;

    @BeforeAll
    static void setUp() {
        allProperty = FileUtils.readAsString(Paths.get("src/test/resources/conf/system.conf"));
        newProperty = FileUtils.readAsString(Paths.get("src/test/resources/conf/spark-defaults.conf"));
        expected = FileUtils.readAsString(Paths.get("src/test/resources/conf/result/default.conf"));
    }

    @Test
    void configWithCurlyBrackets() {
        final String testPropertyWithCurlyBrackets =
                "spark.authenticate=false"+ System.lineSeparator() +
                "spark.sql.hive.metastore.jars=${env:HADOOP_COMMON_HOME}/../hive/lib/*:${env:HADOOP_COMMON_HOME}/client/*";

        SparkDefaultSetter setter = spy(new SparkDefaultSetter());
        doReturn(testPropertyWithCurlyBrackets).when(setter).defaultProperty();
        String result = setter.run(allProperty);
        final String expected = FileUtils.readAsString(Paths.get("src/test/resources/conf/result/default-2.conf"));
        assertEquals(expected, result);
    }

    @Test
    void testRun() {
        SparkDefaultSetter setter = spy(new SparkDefaultSetter());
        doReturn(newProperty).when(setter).defaultProperty();
        String result = setter.run(allProperty);

        assertEquals(expected, result);
    }

    @Test
    void testFileNotFound() {
        SparkDefaultSetter setter = new SparkDefaultSetter();
        assertThrows(IllegalStateException.class, () -> setter.run(allProperty));
    }
}