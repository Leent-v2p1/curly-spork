package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.properties.builder;

import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.DatamartProperties;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.YamlPropertiesParser;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.ActionConf;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.properties.builder.SparkPropertiesBuilderTestHelper.createConf;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.shell.builders.ShellBuilderTestHelper.FULL_TABLE;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.shell.builders.ShellBuilderTestHelper.SCHEMA;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.shell.builders.ShellBuilderTestHelper.TABLE;

class SparkHelperPropertiesBuilderTest {

    @Test
    void testCreateHelperProperties() {
        ActionConf actionConf = createConf();
        HashMap<String, String> customJavaOpts = new HashMap<>();
        customJavaOpts.put("resultSchema", SCHEMA);
        customJavaOpts.put("resultTable", TABLE);
        actionConf.setCustomJavaOpts(customJavaOpts);

        Map<String, Object> yamlConf = new YamlPropertiesParser().parse("/yaml_config/test-properties.yaml");
        DatamartProperties properties = new DatamartProperties(FullTableName.of(FULL_TABLE), yamlConf);

        SparkHelperPropertiesBuilder builder = new SparkHelperPropertiesBuilder(FULL_TABLE,
                actionConf, Environment.PRODUCTION, properties);

        String systemProperties = builder.buildSystemProperties();

        assertTrue(systemProperties.contains("spark.result.schema=custom_rb_test"));
        assertTrue(systemProperties.contains("spark.result.table=test"));
    }
}
