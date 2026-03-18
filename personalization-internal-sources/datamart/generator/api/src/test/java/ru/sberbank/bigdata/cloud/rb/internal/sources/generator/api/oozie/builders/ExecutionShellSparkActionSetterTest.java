package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.builders;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.WorkflowNameEscaper;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.source.workflow.ACTION;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ExecutionShellSparkActionSetterTest {

    @Nested
    class GetAdditionalFiles {
        @Test
        void withModuleJar() {
            ExecutionShellSparkActionSetter actionSetter = new ExecutionShellSparkActionSetter(Environment.PRODUCTION);
            final ACTION action = new ACTION();
            String actionName = "custom_rb_cod.table_reserve";
            action.setName(WorkflowNameEscaper.escape(actionName));
            actionSetter.setAction(action, actionName);

            final List<String> files = action.getShell().getFile();
            final List<String> expected = Arrays.asList(
                    "${wf:appPath()}/custom-rb-cod-table-reserve-runner.sh#custom-rb-cod-table-reserve-runner.sh",
                    "${nameNode}/oozie-app/wf/custom/rb/production/utils/common/jars/personalization.jar",
                    "${nameNode}/oozie-app/wf/custom/rb/production/utils/libs/greenplum-connector-apache-spark-scala_2.11-2.1.0.jar",
                    "${nameNode}/oozie-app/wf/custom/rb/production/utils/libs/postgresql-42.0.0.jre6.jar",
                    "${keytabPath}#${replaceAll(keytabPath, '/keytab/', '')}",
                    "${nameNode}/oozie-app/wf/custom/rb/production/utils/common/custom-log4j.properties#custom-log4j.properties",
                    "${wf:appPath()}/custom-rb-cod-table-reserve-properties-spark.conf#custom-rb-cod-table-reserve-properties-spark.conf",
                    "${wf:appPath()}/custom-rb-cod-table-reserve-properties-system.conf#custom-rb-cod-table-reserve-properties-system.conf",
                    "/oozie-app/wf/custom/rb/production/utils/common/scheduler-pool.xml",
                    "/oozie-app/wf/custom/rb/production/cod/cod-datamart.jar");

            assertEquals(files, expected);
        }

        @Test
        void withoutModuleJar() {
            ExecutionShellSparkActionSetter actionSetter = new ExecutionShellSparkActionSetter(Environment.PRODUCTION);
            final ACTION action = new ACTION();
            String actionName = "custom_rb_cod.table_repartitioner";
            action.setName(WorkflowNameEscaper.escape(actionName));
            actionSetter.setAction(action, actionName);

            final List<String> files = action.getShell().getFile();
            final List<String> expected = Arrays.asList(
                    "${wf:appPath()}/custom-rb-cod-table-repartitioner-runner.sh#custom-rb-cod-table-repartitioner-runner.sh",
                    "${nameNode}/oozie-app/wf/custom/rb/production/utils/common/jars/personalization.jar",
                    "${nameNode}/oozie-app/wf/custom/rb/production/utils/libs/greenplum-connector-apache-spark-scala_2.11-2.1.0.jar",
                    "${nameNode}/oozie-app/wf/custom/rb/production/utils/libs/postgresql-42.0.0.jre6.jar",
                    "${keytabPath}#${replaceAll(keytabPath, '/keytab/', '')}",
                    "${nameNode}/oozie-app/wf/custom/rb/production/utils/common/custom-log4j.properties#custom-log4j.properties",
                    "${wf:appPath()}/custom-rb-cod-table-repartitioner-properties-spark.conf#custom-rb-cod-table-repartitioner-properties-spark.conf",
                    "${wf:appPath()}/custom-rb-cod-table-repartitioner-properties-system.conf#custom-rb-cod-table-repartitioner-properties-system.conf",
                    "/oozie-app/wf/custom/rb/production/utils/common/scheduler-pool.xml");

            assertEquals(files, expected);
        }
    }
}