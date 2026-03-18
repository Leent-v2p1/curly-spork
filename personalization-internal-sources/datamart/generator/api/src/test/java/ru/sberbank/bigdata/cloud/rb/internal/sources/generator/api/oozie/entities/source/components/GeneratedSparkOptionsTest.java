package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.source.components;

import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.DatamartProperties;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.MemoryParams;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.source.components.properties_generators.GeneratedSparkOptions;

import java.util.HashMap;
import java.util.Map;

import static java.lang.System.lineSeparator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.NameAdditions.STG_POSTFIX;

class GeneratedSparkOptionsTest {
    private final String expectedMinimalParamsList = lineSeparator() +
            "                --executor-memory 1G" + lineSeparator() +
            "                --executor-cores 1" + lineSeparator() +
            "                --num-executors 1" + lineSeparator() +
            "                --driver-memory 1G" + lineSeparator() +
            "                --files ${nameNode}${keytabPath}" + lineSeparator() +
            "                --queue ${yarnQueue}" + lineSeparator() +
            "                --conf spark.yarn.maxAppAttempts=1" + lineSeparator() +
            "                --conf spark.driver.extraJavaOptions=" + lineSeparator() +
            "                    -Dhive.metastore.uris=${hiveMetastoreUri}" + lineSeparator() +
            "                    -Dspark.graphite.carbon.host=${graphite.carbon.host}" + lineSeparator() +
            "                    -Dspark.graphite.carbon.port=${graphite.carbon.port}" + lineSeparator() +
            "                    -Dhive.metastore.sasl.enabled=true" + lineSeparator() +
            "                    -Dspark.hiveJdbcUrl=${hiveJdbcUrl}" + lineSeparator() +
            "                    -Dspark.principal=${principal}" + lineSeparator() +
            "                    -Dspark.user.name=${wf:conf('user.name')}" + lineSeparator() +
            "                    -Dspark.keytabPath=${replaceAll(keytabPath, '/keytab/', '')}" + lineSeparator() +
            "                    -Dspark.skipIfBuiltToday=${wf:conf('skipIfBuiltToday')}" + lineSeparator() +
            "                    -Dspark.yarn.executor.memoryOverhead=900" + lineSeparator() +
            "                    -Dspark.sql.shuffle.partitions=400" + lineSeparator() +
            "                    -Dspark.yarn.driver.memoryOverhead=1600" + lineSeparator() +
            "                    -Dspark.ctl.url=${ctl}" + lineSeparator() +
            "                    -Dspark.ctl.loading.id=${loading_id}" + lineSeparator() +
            "                    -Dspark.start.time='${loading_start}'" + lineSeparator() +
            "                    -Dspark.result.schema=custom_rb_card_stg_stg" + lineSeparator() +
            "                    -Dspark.result.table=card_agrmnt_hst_generation" + lineSeparator() +
            "                    -Dspark.wf.id=${wf_id}" + lineSeparator() +
            "                    -Dspark.properties.path=${propertiesPath}" + lineSeparator() +
            "                    -Dspark.environment=${environment}" + lineSeparator() +
            "                    -Dspark.recovery.date='${wf:conf('recoveryDate')}'" + lineSeparator() +
            "                    -Dspark.custom.build.date='${wf:conf('customBuildDate')}'" + lineSeparator() +
            "            ";

    private final String expectedViewParamsList = lineSeparator() +
            "                --executor-memory 1G" + lineSeparator() +
            "                --executor-cores 1" + lineSeparator() +
            "                --num-executors 1" + lineSeparator() +
            "                --driver-memory 1G" + lineSeparator() +
            "                --files ${nameNode}${keytabPath}" + lineSeparator() +
            "                --queue ${yarnQueue}" + lineSeparator() +
            "                --conf spark.yarn.maxAppAttempts=1" + lineSeparator() +
            "                --conf spark.driver.extraJavaOptions=" + lineSeparator() +
            "                    -Dhive.metastore.uris=${hiveMetastoreUri}" + lineSeparator() +
            "                    -Dspark.graphite.carbon.host=${graphite.carbon.host}" + lineSeparator() +
            "                    -Dspark.graphite.carbon.port=${graphite.carbon.port}" + lineSeparator() +
            "                    -Dhive.metastore.sasl.enabled=true" + lineSeparator() +
            "                    -Dspark.hiveJdbcUrl=${hiveJdbcUrl}" + lineSeparator() +
            "                    -Dspark.principal=${principal}" + lineSeparator() +
            "                    -Dspark.user.name=${wf:conf('user.name')}" + lineSeparator() +
            "                    -Dspark.keytabPath=${replaceAll(keytabPath, '/keytab/', '')}" + lineSeparator() +
            "                    -Dspark.skipIfBuiltToday=${wf:conf('skipIfBuiltToday')}" + lineSeparator() +
            "                    -Dspark.yarn.executor.memoryOverhead=900" + lineSeparator() +
            "                    -Dspark.sql.shuffle.partitions=400" + lineSeparator() +
            "                    -Dspark.yarn.driver.memoryOverhead=1600" + lineSeparator() +
            "                    -Dspark.ctl.url=${ctl}" + lineSeparator() +
            "                    -Dspark.ctl.loading.id=${loading_id}" + lineSeparator() +
            "                    -Dspark.start.time='${loading_start}'" + lineSeparator() +
            "                    -Dspark.wf.id=${wf_id}" + lineSeparator() +
            "                    -Dspark.properties.path=${propertiesPath}" + lineSeparator() +
            "                    -Dspark.environment=${environment}" + lineSeparator() +
            "                    -Dspark.recovery.date='${wf:conf('recoveryDate')}'" + lineSeparator() +
            "                    -Dspark.custom.build.date='${wf:conf('customBuildDate')}'" + lineSeparator() +
            "            ";

    @Test
    void testSparkOptionsAssignation() {
        //given: id = "custom_rb_loan_stg.loan_agrmnt_hst_generation"

        //when:
        String actualView = createParams("custom_rb_loan_stg.loan_agrmnt_hst_generation");

        //then:
        assertEquals(expectedViewParamsList, actualView);

        //given again: id = "custom_rb_card_stg.acct_state_mnth_increment_generation"

        //when:
        String actualMin = createStgParams("custom_rb_card_stg.card_agrmnt_hst_generation");
        //then:
        assertEquals(expectedMinimalParamsList, actualMin);
    }

    private String createParams(String id) {
        DatamartProperties propertiesService = new DatamartProperties(FullTableName.of(id));
        GeneratedSparkOptions options = new GeneratedSparkOptions(MemoryParams.createAccordingToPropertiesService(propertiesService));

        //when:
        return options.createOptionsAccordingToTemplate();
    }

    private String createStgParams(String id) {
        DatamartProperties properties = new DatamartProperties(FullTableName.of(id));
        Map<String, Object> params = new HashMap<>();
        params.put("resultSchema", properties.getTargetSchema() + STG_POSTFIX);
        params.put("resultTable", properties.getTargetTable());

        GeneratedSparkOptions options = new GeneratedSparkOptions(MemoryParams.createAccordingToPropertiesService(properties),
                params);

        //when:
        return options.createOptionsAccordingToTemplate();
    }
}