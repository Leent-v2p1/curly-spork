package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.properties.builder;

import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.DatamartProperties;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.YamlPropertiesParser;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.ActionConf;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.NameAdditions.RESERVE_POSTFIX;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.shell.builders.ShellBuilderTestHelper.FULL_TABLE;

class SparkReservingPropertiesBuilderTest {

    @Test
    void testCreateReservingProperties() {
        ActionConf conf = SparkPropertiesBuilderTestHelper.createConf();
        Map<String, Object> testProperties = new YamlPropertiesParser().parse("/yaml_config/test-with-reserving-properties.yaml");
        DatamartProperties properties = new DatamartProperties(FullTableName.of(FULL_TABLE), testProperties);
        SparkReservingPropertiesBuilder builder = new SparkReservingPropertiesBuilder(
                SparkPropertiesBuilderTestHelper.FULL_TABLE + RESERVE_POSTFIX, conf, Environment.PRODUCTION,
                properties);

        String systemProperties = builder.buildSystemProperties();
        assertTrue(systemProperties.contains("spark.result.schema=custom_rb_test"));
        assertTrue(systemProperties.contains("spark.result.table=test"));
        assertTrue(systemProperties.contains("spark.result.table=test"));
        assertTrue(systemProperties.contains("spark.kryoserializer.buffer.max=1024m"));
        assertTrue(systemProperties.contains("spark.repartition=enabled"));
    }
}
