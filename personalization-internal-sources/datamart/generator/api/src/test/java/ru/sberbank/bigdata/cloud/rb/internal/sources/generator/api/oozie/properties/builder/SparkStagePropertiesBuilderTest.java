package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.properties.builder;

import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.DatamartProperties;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.YamlPropertiesParser;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.ActionConf;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.shell.builders.ShellBuilderTestHelper;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.properties.builder.SparkPropertiesBuilderTestHelper.FULL_TABLE;

class SparkStagePropertiesBuilderTest {

    @Test
    void testCreateStageProperties() {
        ActionConf conf = SparkPropertiesBuilderTestHelper.createConf();
        Map<String, Object> yamlConf = new YamlPropertiesParser().parse("/yaml_config/test-properties.yaml");
        DatamartProperties properties = new DatamartProperties(FullTableName.of(ShellBuilderTestHelper.FULL_TABLE), yamlConf);
        SparkStagePropertiesBuilder builder = new SparkStagePropertiesBuilder(
                FULL_TABLE, conf, Environment.PRODUCTION, properties);

        String systemProperties = builder.buildSystemProperties();
        assertTrue(systemProperties.contains("spark.result.schema=custom_rb_test"));
        assertTrue(systemProperties.contains("spark.result.table=test"));
    }
}
