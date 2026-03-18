package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.test_api.HadoopEnv;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.util.FileUtils;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.config.ParamSetterRunner;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.config.param_setter.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ConfigGeneratorTest {

    @Test
    void testGeneration() throws IOException {
        HadoopEnv.setEnvVariables();
        Path tempPath = Files.createTempDirectory("config_generator");
        Path tempTemplate = tempPath.resolve("datamart.template");
        Files.createFile(tempTemplate);
        ParamSetterRunner runner = mock(ParamSetterRunner.class);
        when(runner.run(any(), any())).thenReturn("result_conf_content");
        ConfigGenerator configGenerator = new ConfigGenerator(runner);

        configGenerator.generatePropertyFile(emptyList(), tempTemplate.toString());

        String confFileContent = FileUtils.readAsString(tempPath.resolve("datamart.conf"));
        assertEquals("result_conf_content", confFileContent);
    }

    @Test
    void builderSparkProperties() {
        List<ParamSetter> paramSetters = ConfigGenerator.buildSparkProperties(Collections.emptyMap(), "test_datamart");
        assertThat(paramSetters, hasSize(2));
        assertThat(paramSetters.get(0), is(instanceOf(CommonSetter.class)));
        assertThat(paramSetters.get(1), is(instanceOf(ExecutionSetter.class)));
    }

    @Test
    void builderSystemProperties() {
        List<ParamSetter> paramSetters = ConfigGenerator.buildSystemProperties(Collections.emptyMap(), "test_datamart");
        assertThat(paramSetters, hasSize(3));
        assertThat(paramSetters.get(0), is(instanceOf(SparkDefaultSetter.class)));
        assertThat(paramSetters.get(1), is(instanceOf(CommonSetter.class)));
        assertThat(paramSetters.get(2), is(instanceOf(CustomSetter.class)));
    }

    @Nested
    class GetValidPropertiesPath {

        @Test
        void validPath() {
            System.setProperty("key1", "path-to-template.template");
            String value = ConfigGenerator.getValidPropertiesPath("key1");
            assertEquals("path-to-template.template", value);
            System.clearProperty("key1");
        }

        @Test
        void notValidPath() {
            System.setProperty("key1", "path-to-template.conf");
            assertThrows(IllegalArgumentException.class, () -> ConfigGenerator.getValidPropertiesPath("key1"));
            System.clearProperty("key1");
        }
    }
}
