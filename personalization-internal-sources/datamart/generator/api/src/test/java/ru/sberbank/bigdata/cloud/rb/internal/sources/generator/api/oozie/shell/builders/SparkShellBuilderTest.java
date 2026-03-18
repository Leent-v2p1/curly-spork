package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.shell.builders;

import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.ActionConf;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.shell.SparkShellBuilder;

import static java.lang.System.lineSeparator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.shell.builders.ShellBuilderTestHelper.FULL_TABLE;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.shell.builders.ShellBuilderTestHelper.createConf;

class SparkShellBuilderTest {

    private final String EXPECTED =
            "source custom-rb-test-test-properties-spark.conf" + lineSeparator() +
                    "export HADOOP_CONF_DIR=/etc/hive/conf" + lineSeparator() +
                    "" + lineSeparator() +
                    "domain=$(echo $principal | grep -o \"@.*\")" + lineSeparator() +
                    "userPrincipal=\"$user_name$domain\"" + lineSeparator() +
                    "keytab=$(echo $keytabPath | grep -o \"\\w*\\.keytab\")" + lineSeparator() +
                    "" + lineSeparator() +
                    "echo \"domain=$domain\"" + lineSeparator() +
                    "echo \"userPrincipal=$userPrincipal\"" + lineSeparator() +
                    "echo \"keytabPath=$keytabPath\"" + lineSeparator() +
                    "echo \"keytab=$keytab\"" + lineSeparator() +
                    "" + lineSeparator() +
                    "spark2-submit \\" + lineSeparator() +
                    "    --class ru.my.Class \\" + lineSeparator() +
                    "    --properties-file custom-rb-test-test-properties-system.conf \\" + lineSeparator() +
                    "    --executor-memory $executorMemory \\" + lineSeparator() +
                    "    --executor-cores $executorCoreNum \\" + lineSeparator() +
                    "    --num-executors $executors \\" + lineSeparator() +
                    "    --driver-memory $driverMemory \\" + lineSeparator() +
                    "    --principal $userPrincipal \\" + lineSeparator() +
                    "    --keytab $keytab \\" + lineSeparator() +
                    "    --files custom-log4j.properties \\" + lineSeparator() +
                    "    --master yarn \\" + lineSeparator() +
                    "    --queue $queue \\" + lineSeparator() +
                    "    --deploy-mode client \\" + lineSeparator() +
                    "    --jars personalization.jar,test-datamart.jar\\" + lineSeparator() +
                    "    --conf spark.driver.extraClassPath=personalization.jar:test-datamart.jar:\\" + lineSeparator() +
                    "/opt/cloudera/parcels/CDH/lib/hive/lib/hive-jdbc.jar:/opt/cloudera/parcels/CDH/lib/hive/lib/hive-service.jar \\" + lineSeparator() +
                    "    --conf spark.extraListeners=ru.sberbank.bigdata.cloud.rb.internal.sources.common.diagnostic.TaskInfoRecorderListener \\" + lineSeparator() +
                    "    --conf spark.dynamicAllocation.enabled=false \\" + lineSeparator() +
                    "    --conf \"spark.driver.extraJavaOptions= \\" + lineSeparator() +
                    "        -Dhive.metastore.uris=$metastore_uri \\" + lineSeparator() +
                    "        -Dhive.metastore.sasl.enabled=true \\" + lineSeparator() +
                    "        -Dlog4j.configuration=file:./custom-log4j.properties \\" + lineSeparator() +
                    "\" \\" + lineSeparator() +
                    "test-datamart.jar";

    @Test
    void generateExpectedShellScript() {
        SparkShellBuilder<ActionConf> builder = new SparkShellBuilder<>(FULL_TABLE, createConf());
        assertEquals(EXPECTED, builder.buildShell());
    }
}
