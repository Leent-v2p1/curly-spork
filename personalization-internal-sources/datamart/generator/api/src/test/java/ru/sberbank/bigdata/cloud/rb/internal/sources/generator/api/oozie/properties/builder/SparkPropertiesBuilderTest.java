package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.properties.builder;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.DatamartProperties;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.YamlPropertiesParser;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.ActionConf;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.shell.builders.ShellBuilderTestHelper;

import java.util.HashMap;
import java.util.Map;

import static java.lang.System.lineSeparator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SparkPropertiesBuilderTest {

    public static final String EXPECTED_WITH_CUSTOM = "# Spark default parameters" + lineSeparator() +
            "[sparkAppDefaultConf]" + lineSeparator() +
            "spark.sql.autoBroadcastJoinThreshold=524288000" + lineSeparator() +
            "# Generated parameters" + lineSeparator() +
            "spark.yarn.executor.memoryOverhead=900" + lineSeparator() +
            "spark.sql.shuffle.partitions=400" + lineSeparator() +
            "spark.yarn.maxAppAttempts=1" + lineSeparator() +
            "spark.datamart.class.name=ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.erib.SbolLogon" + lineSeparator() +
            "spark.source.schema=internal_erib_srb_ikfl6" + lineSeparator() +
            "spark.result.schema=custom_rb_sbol" + lineSeparator() +
            "spark.result.table=sbol_logon_ikfl6" + lineSeparator() +
            "spark.ctl.entity.id=901031302" + lineSeparator() +
            "spark.workflow.type=DATAMART" + lineSeparator() +
            "spark.datamart.parent.table.name=custom_rb_sbol.sbol_logon_ikfl6" + lineSeparator() +
            "spark.repartition=enabled" + lineSeparator() +
            "spark.memory.fraction=0.5" + lineSeparator() +
            "# Parameters from ctl" + lineSeparator() +
            "spark.graphite.carbon.host=" + lineSeparator() +
            "spark.graphite.carbon.port=$[graphite.carbon.port]" + lineSeparator() +
            "spark.skipIfBuiltToday=$[skipIfBuiltToday]" + lineSeparator() +
            "spark.hiveJdbcUrl=$[hiveJdbcUrl]" + lineSeparator() +
            "spark.principal=$[principal]" + lineSeparator() +
            "spark.user.name=$[user.name]" + lineSeparator() +
            "spark.keytabPath=$[keytabPath]" + lineSeparator() +
            "spark.ctl.url=$[ctl.url]" + lineSeparator() +
            "spark.ctl.loading.id=$[ctl.loading.id]" + lineSeparator() +
            "spark.wf.id=$[ctl.wf.id]" + lineSeparator() +
            "spark.start.time=$[ctl.loading.start]" + lineSeparator() +
            "spark.properties.path=$[propertiesPath]" + lineSeparator() +
            "spark.environment=$[environment]" + lineSeparator() +
            "spark.recovery.date=$[recoveryDate]" + lineSeparator() +
            "spark.custom.build.date=$[custom.build.date]" + lineSeparator() +
            "spark.start_dt=$[start_dt]" + lineSeparator() +
            "# Custom parameters from ctl" + lineSeparator() +
            "[ctlCustomConf]";
    private final String EXPECTED_SPARK = "# Generated parameters" + lineSeparator() +
            "executorMemory=4G" + lineSeparator() +
            "executorCoreNum=1" + lineSeparator() +
            "executors=50" + lineSeparator() +
            "driverMemory=2G" + lineSeparator() +
            "# Parameters from ctl" + lineSeparator() +
            "principal=$[principal]" + lineSeparator() +
            "keytabPath=$[keytabPath]" + lineSeparator() +
            "queue=$[yarnQueue]" + lineSeparator() +
            "metastore_uri=$[hiveMetastoreUri]" + lineSeparator() +
            "user_name=$[user.name]" + lineSeparator() +
            "# Additional ctl execution parameters" + lineSeparator() +
            "[ctlExecutionConf]";
    private final String EXPECTED_SYSTEM = "# Spark default parameters" + lineSeparator() +
            "[sparkAppDefaultConf]" + lineSeparator() +
            "spark.sql.autoBroadcastJoinThreshold=524288000" + lineSeparator() +
            "# Generated parameters" + lineSeparator() +
            "spark.yarn.executor.memoryOverhead=400" + lineSeparator() +
            "spark.sql.shuffle.partitions=400" + lineSeparator() +
            "spark.yarn.maxAppAttempts=1" + lineSeparator() +
            "spark.class.name=ru.my.Class" + lineSeparator() +
            "spark.result.schema=custom_rb_test" + lineSeparator() +
            "spark.result.table=test" + lineSeparator() +
            "spark.workflow.type=DATAMART" + lineSeparator() +
            "spark.datamart.parent.table.name=custom_rb_test.test" + lineSeparator() +
            "# Parameters from ctl" + lineSeparator() +
            "spark.graphite.carbon.host=" + lineSeparator() +
            "spark.graphite.carbon.port=$[graphite.carbon.port]" + lineSeparator() +
            "spark.skipIfBuiltToday=$[skipIfBuiltToday]" + lineSeparator() +
            "spark.hiveJdbcUrl=$[hiveJdbcUrl]" + lineSeparator() +
            "spark.principal=$[principal]" + lineSeparator() +
            "spark.user.name=$[user.name]" + lineSeparator() +
            "spark.keytabPath=$[keytabPath]" + lineSeparator() +
            "spark.ctl.url=$[ctl.url]" + lineSeparator() +
            "spark.ctl.loading.id=$[ctl.loading.id]" + lineSeparator() +
            "spark.wf.id=$[ctl.wf.id]" + lineSeparator() +
            "spark.start.time=$[ctl.loading.start]" + lineSeparator() +
            "spark.properties.path=$[propertiesPath]" + lineSeparator() +
            "spark.environment=$[environment]" + lineSeparator() +
            "spark.recovery.date=$[recoveryDate]" + lineSeparator() +
            "spark.custom.build.date=$[custom.build.date]" + lineSeparator() +
            "spark.start_dt=$[start_dt]" + lineSeparator() +
            "# Custom parameters from ctl" + lineSeparator() +
            "[ctlCustomConf]";
    private DatamartProperties properties;

    @BeforeEach
    void setUp() {
        Map<String, Object> yamlConf = new YamlPropertiesParser().parse("/yaml_config/test-properties.yaml");
        properties = new DatamartProperties(FullTableName.of(ShellBuilderTestHelper.FULL_TABLE), yamlConf);
    }

    @Test
    void generateExpectedSparkProperties() {
        SparkPropertiesBuilder<ActionConf> builder = new SparkPropertiesBuilder<>(ShellBuilderTestHelper.FULL_TABLE,
                ShellBuilderTestHelper.createConf(), Environment.PRODUCTION, properties);
        String sparkProperties = builder.buildSparkProperties();
        String systemProperties = builder.buildSystemProperties();
        assertEquals(EXPECTED_SPARK, sparkProperties);
        assertEquals(EXPECTED_SYSTEM, systemProperties);
    }

    @Test
    void testAppendSparkPrefix() {
        SparkPropertiesBuilder<ActionConf> builder = new SparkPropertiesBuilder<>(ShellBuilderTestHelper.FULL_TABLE,
                ShellBuilderTestHelper.createConf(), Environment.PRODUCTION, properties);
        Map<String, String> properties = new HashMap<>();
        properties.put("partitioner", "true");
        properties.put("result.size", "40");

        Map<String, String> result = builder.addSparkPrefix(properties);

        assertEquals(2, result.size());
        assertTrue(result.containsKey("spark.partitioner"));
        assertTrue(result.containsKey("spark.result.size"));
    }

    @Test
    void buildSystemPropertiesWithCustomJavaOptions() {
        final FullTableName id = FullTableName.of("custom_rb_sbol.sbol_logon_ikfl6");
        Map<String, Object> testProperties = new YamlPropertiesParser().parse("/yaml_config/test-d-p-with-custom-options.yaml");
        final DatamartProperties datamartProperties = new DatamartProperties(id, testProperties);
        final SparkPropertiesBuilder<ActionConf> actionConfSparkPropertiesBuilder = new SparkPropertiesBuilder<>(
                id.fullTableName(),
                new ActionConf(),
                Environment.PRODUCTION,
                datamartProperties);
        final String result = actionConfSparkPropertiesBuilder.buildSystemProperties();
        assertEquals(EXPECTED_WITH_CUSTOM, result);
    }
}
